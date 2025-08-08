import sqlite3
from tkinter import messagebox
import logging
import os
import shutil
from datetime import datetime, timedelta

from utils.date_utils import get_holidays_set_for_period, jours_ouvres, validate_date
from utils.config_loader import CONFIG
from db.models import Agent, Conge


class CongeManager:
    def __init__(self, db_manager, certificats_dir):
        self.db = db_manager
        self.certificats_dir = certificats_dir

    def get_all_agents(self, **kwargs):
        return self.db.get_agents(**kwargs)

    def get_agent_by_id(self, agent_id):
        return self.db.get_agent_by_id(agent_id)

    def save_agent(self, agent_data, is_modification=False):
        if is_modification:
            return self.db.modifier_agent(
                agent_data['id'], agent_data['nom'], agent_data['prenom'],
                agent_data['ppr'], agent_data['grade'], agent_data['solde']
            )
        else:
            return self.db.ajouter_agent(
                agent_data['nom'], agent_data['prenom'], agent_data['ppr'],
                agent_data['grade'], agent_data['solde']
            )

    def delete_agent_with_confirmation(self, agent_id, agent_nom):
        if messagebox.askyesno("Confirmation", f"Supprimer l'agent '{agent_nom}' et tous ses congés ?\nCette action est irréversible."):
            return self.db.supprimer_agent(agent_id)
        return False

    def get_conges_for_agent(self, agent_id):
        return self.db.get_conges(agent_id=agent_id)
        
    def get_conge_by_id(self, conge_id):
        return self.db.get_conge_by_id(conge_id)
    
    def delete_conge_with_confirmation(self, conge_id):
        if messagebox.askyesno("Confirmation", "Êtes-vous sûr de vouloir supprimer ce congé ?\nS'il fait partie d'une division, l'opération sera annulée et le congé d'origine sera restauré."):
            try:
                return self.revoke_split_on_delete(conge_id)
            except Exception as e:
                logging.error(f"Erreur lors de la suppression du congé {conge_id}: {e}", exc_info=True)
                messagebox.showerror("Erreur Inattendue", f"Une erreur est survenue : {e}")
                return False
        return False

    def revoke_split_on_delete(self, conge_id_to_delete):
        logging.info(f"Début de la suppression pour le congé ID {conge_id_to_delete}.")
        conge_to_delete = self.db.get_conge_by_id(conge_id_to_delete)
        if not conge_to_delete: return False

        agent_id = conge_to_delete.agent_id
        
        cursor = self.db.conn.cursor()
        try:
            cursor.execute('BEGIN TRANSACTION')
            parent_conge_cursor = cursor.execute(
                "SELECT * FROM conges WHERE agent_id = ? AND type_conge = 'Congé annuel' AND statut = 'Annulé' AND date_debut <= ? AND date_fin >= ?",
                (agent_id, conge_to_delete.date_debut.strftime('%Y-%m-%d'), conge_to_delete.date_fin.strftime('%Y-%m-%d'))
            )
            parent_conge_row = parent_conge_cursor.fetchone()

            if parent_conge_row:
                parent_conge = Conge.from_db_row(parent_conge_row)
                logging.info(f"Annulation de division détectée. Parent ID: {parent_conge.id}.")
                
                fragments_cursor = cursor.execute(
                    "SELECT id FROM conges WHERE agent_id = ? AND statut = 'Actif' AND date_debut >= ? AND date_fin <= ?",
                    (agent_id, parent_conge.date_debut.strftime('%Y-%m-%d'), parent_conge.date_fin.strftime('%Y-%m-%d'))
                )
                fragment_ids_to_delete = [row[0] for row in fragments_cursor.fetchall()]

                logging.info(f"Suppression des fragments IDs: {fragment_ids_to_delete}")
                for frag_id in fragment_ids_to_delete:
                    self.db._supprimer_conge_no_commit(cursor, frag_id)
                
                logging.info(f"Réactivation du congé parent ID: {parent_conge.id}")
                cursor.execute("UPDATE conges SET statut = 'Actif' WHERE id = ?", (parent_conge.id,))
                if parent_conge.type_conge in CONFIG['conges']['types_decompte_solde']:
                    cursor.execute("UPDATE agents SET solde = solde - ? WHERE id = ?", (parent_conge.jours_pris, agent_id))
            else:
                logging.info(f"Suppression simple du congé ID: {conge_id_to_delete}.")
                self.db._supprimer_conge_no_commit(cursor, conge_id_to_delete)

            self.db.conn.commit()
            return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback()
            logging.error(f"Échec de la transaction de suppression: {e}", exc_info=True)
            raise e

    def handle_conge_submission(self, form_data, is_modification):
        try:
            start_date = validate_date(form_data['date_debut'])
            end_date = validate_date(form_data['date_fin'])
            if not all([form_data['type_conge'], start_date, end_date]) or end_date < start_date or form_data['jours_pris'] <= 0:
                raise ValueError("Veuillez vérifier le type, les dates et la durée du congé.")

            conge_id_exclu = form_data['conge_id'] if is_modification else None
            overlaps = self.db.get_overlapping_leaves(form_data['agent_id'], start_date, end_date, conge_id_exclu)
            
            if overlaps:
                annual_overlap = next((c for c in overlaps if c.type_conge == 'Congé annuel'), None)
                if form_data['type_conge'] != 'Congé annuel' and annual_overlap and len(overlaps) == 1:
                    if messagebox.askyesno("Confirmation de Remplacement", "Ce congé chevauche un congé annuel. Voulez-vous le remplacer ?"):
                        return self.split_and_replace_annual_leave(annual_overlap, form_data)
                    else: return False
                else: raise ValueError("Les dates sélectionnées chevauchent un congé déjà existant.")
            
            conge_model = Conge(
                id=form_data.get('conge_id'), agent_id=form_data['agent_id'], type_conge=form_data['type_conge'],
                justif=form_data.get('justif'), interim_id=form_data.get('interim_id'), date_debut=start_date.strftime('%Y-%m-%d'),
                date_fin=end_date.strftime('%Y-%m-%d'), jours_pris=form_data['jours_pris']
            )
            
            if is_modification: conge_id = self.db.modifier_conge(conge_model.id, conge_model)
            else: conge_id = self.db.ajouter_conge(conge_model)

            if conge_id and form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, is_modification, conge_id)
            return True if conge_id else False
        except (ValueError, sqlite3.Error) as e:
            messagebox.showerror("Erreur de validation", str(e)); return False
        except Exception as e:
            logging.error(f"Erreur soumission congé: {e}", exc_info=True)
            messagebox.showerror("Erreur Inattendue", str(e)); return False

    def _handle_certificat_save(self, form_data, is_modification, conge_id):
        new_path = form_data['cert_path']
        original_path = form_data['original_cert_path']
        valid_conge_id = conge_id or form_data.get('conge_id')

        if new_path and os.path.exists(new_path) and new_path != original_path:
            try:
                filename = f"cert_{form_data['agent_ppr']}_{valid_conge_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{os.path.splitext(new_path)[1]}"
                dest_path = os.path.join(self.certificats_dir, filename)
                shutil.copy(new_path, dest_path)
                
                certificat_model = type('Certificat', (object,), {'nom_medecin': "", 'duree_jours': form_data['jours_pris'], 'chemin_fichier': dest_path})()
                cursor = self.db.conn.cursor()
                self.db._add_or_update_certificat_no_commit(cursor, valid_conge_id, certificat_model)
                self.db.conn.commit()

                if original_path and os.path.exists(original_path): os.remove(original_path)
            except Exception as e:
                self.db.conn.rollback()
                logging.error(f"Erreur sauvegarde certificat: {e}", exc_info=True)
                messagebox.showwarning("Erreur Certificat", f"Le congé a été sauvegardé, mais le certificat n'a pas pu être copié:\n{e}")
        elif not new_path and original_path:
            try:
                self.db.execute_query("DELETE FROM certificats_medicaux WHERE conge_id = ?", (valid_conge_id,))
                if os.path.exists(original_path): os.remove(original_path)
            except Exception as e:
                logging.error(f"Impossible de supprimer l'ancien certificat pour conge_id {conge_id}: {e}")

    def split_and_replace_annual_leave(self, annual_overlap, form_data):
        agent_id = form_data['agent_id']
        logging.info(f"Début du processus de division pour l'agent ID {agent_id}.")
        try:
            old_conge_id = annual_overlap.id
            old_start, old_end, old_days = annual_overlap.date_debut, annual_overlap.date_fin, annual_overlap.jours_pris
            new_start, new_end = validate_date(form_data['date_debut']), validate_date(form_data['date_fin'])
            holidays_set = get_holidays_set_for_period(self.db, old_start.year, new_end.year + 2)
            
            days1 = jours_ouvres(old_start, new_start - timedelta(days=1), holidays_set) if old_start < new_start else 0
            days2 = jours_ouvres(new_end + timedelta(days=1), old_end, holidays_set) if old_end > new_end else 0

            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (old_conge_id,))
            if annual_overlap.type_conge in CONFIG['conges']['types_decompte_solde']:
                cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (old_days, agent_id))

            new_conge_model = Conge(
                id=None, agent_id=agent_id, type_conge=form_data['type_conge'], justif=form_data['justif'],
                interim_id=form_data['interim_id'], date_debut=new_start.strftime('%Y-%m-%d'),
                date_fin=new_end.strftime('%Y-%m-%d'), jours_pris=form_data['jours_pris']
            )
            new_conge_id = self.db._ajouter_conge_no_commit(cursor, new_conge_model)

            if form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, False, new_conge_id)

            if days1 > 0:
                seg1 = Conge(id=None, agent_id=agent_id, type_conge='Congé annuel', justif=None, interim_id=None,
                             date_debut=old_start.strftime('%Y-%m-%d'), date_fin=(new_start - timedelta(days=1)).strftime('%Y-%m-%d'),
                             jours_pris=int(days1))
                self.db._ajouter_conge_no_commit(cursor, seg1)
            if days2 > 0:
                seg2 = Conge(id=None, agent_id=agent_id, type_conge='Congé annuel', justif=None, interim_id=None,
                             date_debut=(new_end + timedelta(days=1)).strftime('%Y-%m-%d'), date_fin=old_end.strftime('%Y-%m-%d'),
                             jours_pris=int(days2))
                self.db._ajouter_conge_no_commit(cursor, seg2)
            
            self.db.conn.commit()
            logging.info(f"Transaction de division pour congé {old_conge_id} terminée.")
            return True
        except (sqlite3.Error, OSError, ValueError) as e:
            self.db.conn.rollback()
            logging.error(f"Échec transaction split_and_replace_annual_leave: {e}", exc_info=True)
            raise e