# Fichier : core/conges/manager.py (Version finale avec la logique de remplacement unifiée)

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
        logging.info(f"Début de la suppression/restauration pour le congé ID {conge_id_to_delete}.")
        conge_to_delete = self.db.get_conge_by_id(conge_id_to_delete)
        if not conge_to_delete: return False

        agent_id = conge_to_delete.agent_id
        
        try:
            parent_conge_row = self.db.execute_query(
                """SELECT * FROM conges 
                   WHERE agent_id = ? AND type_conge = 'Congé annuel' AND statut = 'Annulé' 
                   AND date(date_debut) <= date(?) AND date(date_fin) >= date(?)
                   ORDER BY date_debut DESC 
                   LIMIT 1""",
                (agent_id, conge_to_delete.date_debut.strftime('%Y-%m-%d'), conge_to_delete.date_fin.strftime('%Y-%m-%d')),
                fetch="one"
            )

            if parent_conge_row:
                parent_conge = Conge.from_db_row(parent_conge_row)
                logging.info(f"Restauration détectée. Parent ID: {parent_conge.id}.")
                
                all_active_conges = self.db.get_conges(agent_id=agent_id)
                fragment_ids_to_delete = []
                parent_start_date = parent_conge.date_debut.date()
                parent_end_date = parent_conge.date_fin.date()

                for conge in all_active_conges:
                    if conge.statut == 'Actif':
                        conge_start_date = conge.date_debut.date()
                        conge_end_date = conge.date_fin.date()
                        if conge_start_date >= parent_start_date and conge_end_date <= parent_end_date:
                            fragment_ids_to_delete.append(conge.id)
                
                if not fragment_ids_to_delete:
                    messagebox.showwarning("Opération Annulée", "La logique de restauration a échoué car aucun fragment actif n'a été trouvé dans la période du congé parent.")
                    return False

                self.db.conn.execute('BEGIN TRANSACTION')
                cursor = self.db.conn.cursor()

                logging.info(f"Suppression des fragments IDs: {fragment_ids_to_delete}")
                for frag_id in fragment_ids_to_delete:
                    self.db._supprimer_conge_no_commit(cursor, frag_id)
                
                logging.info(f"Réactivation du congé parent ID: {parent_conge.id}")
                cursor.execute("UPDATE conges SET statut = 'Actif' WHERE id = ?", (parent_conge.id,))
                
                if parent_conge.type_conge in CONFIG['conges']['types_decompte_solde']:
                    cursor.execute("UPDATE agents SET solde = solde - ? WHERE id = ?", (parent_conge.jours_pris, agent_id))

                self.db.conn.commit()
                return True
            else:
                logging.info(f"Aucun parent trouvé. Suppression simple du congé ID: {conge_id_to_delete}.")
                self.db.supprimer_conge(conge_id_to_delete)
                return True
        except (sqlite3.Error, ValueError) as e:
            if self.db.conn.in_transaction: self.db.conn.rollback()
            logging.error(f"Échec de la transaction de suppression/restauration: {e}", exc_info=True)
            raise e

    def handle_conge_submission(self, form_data, is_modification):
        try:
            start_date = validate_date(form_data['date_debut'])
            end_date = validate_date(form_data['date_fin'])
            if not all([form_data['type_conge'], start_date, end_date]) or end_date < start_date or form_data['jours_pris'] <= 0:
                raise ValueError("Veuillez vérifier le type, les dates et la durée du congé.")

            conge_id_exclu = form_data.get('conge_id') if is_modification else None
            overlaps = self.db.get_overlapping_leaves(form_data['agent_id'], start_date, end_date, conge_id_exclu)
            
            if overlaps:
                annual_overlaps = [c for c in overlaps if c.type_conge == 'Congé annuel']
                if form_data['type_conge'] == 'Congé annuel' or len(annual_overlaps) != len(overlaps):
                    raise ValueError("Chevauchement invalide. Vous ne pouvez remplacer des congés annuels que par un autre type de congé (ex: maladie).")

                if messagebox.askyesno("Confirmation de Remplacement", "Ce congé va remplacer ou diviser un ou plusieurs congés annuels. Continuer ?"):
                    return self.split_or_replace_leaves(annual_overlaps, form_data)
                else:
                    return False

            # S'il n'y a pas de chevauchement, on procède normalement
            conge_model = Conge(id=form_data.get('conge_id'), agent_id=form_data['agent_id'], type_conge=form_data['type_conge'],
                                justif=form_data.get('justif'), interim_id=form_data.get('interim_id'), 
                                date_debut=start_date.strftime('%Y-%m-%d'), date_fin=end_date.strftime('%Y-%m-%d'), 
                                jours_pris=form_data['jours_pris'])
            
            if is_modification:
                conge_id = self.db.modifier_conge(form_data['conge_id'], conge_model)
            else:
                conge_id = self.db.ajouter_conge(conge_model)

            if conge_id and form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, is_modification, conge_id)
            return True if conge_id else False

        except (ValueError, sqlite3.Error) as e:
            messagebox.showerror("Erreur de validation", str(e)); return False
        except Exception as e:
            logging.error(f"Erreur soumission congé: {e}", exc_info=True)
            messagebox.showerror("Erreur Inattendue", str(e)); return False

    def split_or_replace_leaves(self, annual_overlaps, form_data):
        """
        Annule les congés annuels chevauchés et recrée les fragments nécessaires autour du nouveau congé.
        """
        logging.info(f"Division/Remplacement de {len(annual_overlaps)} congés annuels.")
        try:
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            
            new_start = validate_date(form_data['date_debut'])
            new_end = validate_date(form_data['date_fin'])
            holidays_set = get_holidays_set_for_period(self.db, new_start.year - 1, new_end.year + 2)

            # Pour chaque congé annuel existant qui est chevauché...
            for conge in annual_overlaps:
                # 1. On l'annule au lieu de le supprimer, et on restitue les jours au solde.
                # C'est une étape cruciale pour préserver l'historique et la cohérence.
                cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (conge.id,))
                if conge.type_conge in CONFIG['conges']['types_decompte_solde']:
                    cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (conge.jours_pris, conge.agent_id))

                # 2. On recrée le segment AVANT le nouveau congé, si nécessaire.
                if conge.date_debut < new_start:
                    end_part1 = new_start - timedelta(days=1)
                    days_part1 = jours_ouvres(conge.date_debut, end_part1, holidays_set)
                    if days_part1 > 0:
                        seg1 = Conge(None, conge.agent_id, 'Congé annuel', None, None, conge.date_debut.strftime('%Y-%m-%d'), end_part1.strftime('%Y-%m-%d'), int(days_part1))
                        self.db._ajouter_conge_no_commit(cursor, seg1)

                # 3. On recrée le segment APRÈS le nouveau congé, si nécessaire.
                if conge.date_fin > new_end:
                    start_part2 = new_end + timedelta(days=1)
                    days_part2 = jours_ouvres(start_part2, conge.date_fin, holidays_set)
                    if days_part2 > 0:
                        seg2 = Conge(None, conge.agent_id, 'Congé annuel', None, None, start_part2.strftime('%Y-%m-%d'), conge.date_fin.strftime('%Y-%m-%d'), int(days_part2))
                        self.db._ajouter_conge_no_commit(cursor, seg2)

            # 4. Enfin, on ajoute le nouveau congé (maladie, etc.) qui a provoqué la division/remplacement.
            new_conge_model = Conge(
                id=None, agent_id=form_data['agent_id'], type_conge=form_data['type_conge'],
                justif=form_data.get('justif'), interim_id=form_data.get('interim_id'),
                date_debut=new_start.strftime('%Y-%m-%d'), date_fin=new_end.strftime('%Y-%m-%d'),
                jours_pris=form_data['jours_pris']
            )
            new_conge_id = self.db._ajouter_conge_no_commit(cursor, new_conge_model)

            if new_conge_id and form_data['type_conge'] == "Congé de maladie":
                self._handle_certificat_save(form_data, False, new_conge_id)

            self.db.conn.commit()
            return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback()
            raise e

    def _handle_certificat_save(self, form_data, is_modification, conge_id):
        new_path = form_data.get('cert_path')
        original_path = form_data.get('original_cert_path')
        if not new_path or not conge_id: return

        if os.path.exists(new_path) and new_path != original_path:
            try:
                filename = f"cert_{form_data['agent_ppr']}_{conge_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{os.path.splitext(new_path)[1]}"
                dest_path = os.path.join(self.certificats_dir, filename)
                shutil.copy(new_path, dest_path)
                
                cert_model = type('Certificat', (object,), {'nom_medecin': "", 'duree_jours': form_data['jours_pris'], 'chemin_fichier': dest_path})()
                self.db.execute_query("REPLACE INTO certificats_medicaux (conge_id, duree_jours, chemin_fichier) VALUES (?, ?, ?)",
                                      (conge_id, cert_model.duree_jours, cert_model.chemin_fichier))

                if original_path and os.path.exists(original_path): os.remove(original_path)
            except Exception as e:
                logging.error(f"Erreur sauvegarde certificat: {e}", exc_info=True)
                messagebox.showwarning("Erreur Certificat", f"Le congé a été sauvegardé, mais le certificat n'a pas pu être copié:\n{e}")
        elif not new_path and original_path:
            try:
                self.db.execute_query("DELETE FROM certificats_medicaux WHERE conge_id = ?", (conge_id,))
                if os.path.exists(original_path): os.remove(original_path)
            except Exception as e:
                logging.error(f"Impossible de supprimer l'ancien certificat pour conge_id {conge_id}: {e}")