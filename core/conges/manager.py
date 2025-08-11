# Fichier : core/conges/manager.py (Version finale avec une fonction par cas de figure)

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

    # --- Les fonctions de base ne changent pas ---
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
        # ... (cette fonction ne change pas, elle est stable)
        logging.info(f"Début de la suppression/restauration pour le congé ID {conge_id_to_delete}.")
        conge_to_delete = self.db.get_conge_by_id(conge_id_to_delete)
        if not conge_to_delete: return False
        agent_id = conge_to_delete.agent_id
        try:
            parent_conge_row = self.db.execute_query(
                """SELECT * FROM conges 
                   WHERE agent_id = ? AND type_conge = 'Congé annuel' AND statut = 'Annulé' 
                   AND ( (date(date_debut) <= date(?) AND date(date_fin) >= date(?)) OR (date(date_debut) >= date(?) AND date(date_fin) <= date(?)) )
                   ORDER BY date_debut DESC LIMIT 1""",
                (agent_id, conge_to_delete.date_debut.strftime('%Y-%m-%d'), conge_to_delete.date_fin.strftime('%Y-%m-%d'),
                 conge_to_delete.date_debut.strftime('%Y-%m-%d'), conge_to_delete.date_fin.strftime('%Y-%m-%d')),
                fetch="one"
            )
            if parent_conge_row:
                parent_conge = Conge.from_db_row(parent_conge_row)
                logging.info(f"Restauration détectée. Parent ID: {parent_conge.id}.")
                self.db.conn.execute('BEGIN TRANSACTION')
                cursor = self.db.conn.cursor()
                self.db._supprimer_conge_no_commit(cursor, conge_id_to_delete)
                all_active_conges = [Conge.from_db_row(r) for r in cursor.execute("SELECT * FROM conges WHERE agent_id=? AND statut='Actif'", (agent_id,)).fetchall()]
                for conge in all_active_conges:
                    if conge.date_debut >= parent_conge.date_debut and conge.date_fin <= parent_conge.date_fin:
                         self.db._supprimer_conge_no_commit(cursor, conge.id)
                cursor.execute("UPDATE conges SET statut = 'Actif' WHERE id = ?", (parent_conge.id,))
                if parent_conge.type_conge in CONFIG['conges']['types_decompte_solde']:
                    cursor.execute("UPDATE agents SET solde = solde - ? WHERE id = ?", (parent_conge.jours_pris, agent_id))
                self.db.conn.commit()
                return True
            else:
                logging.info(f"Aucun parent trouvé. Suppression simple.")
                self.db.supprimer_conge(conge_id_to_delete)
                return True
        except (sqlite3.Error, ValueError) as e:
            if self.db.conn.in_transaction: self.db.conn.rollback()
            logging.error(f"Échec de la transaction: {e}", exc_info=True); raise e

    def handle_conge_submission(self, form_data, is_modification):
        """Fonction 'routeur' principale."""
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
                    raise ValueError("Chevauchement invalide. Vous ne pouvez remplacer des congés annuels que par un autre type de congé.")

                # Simplification: on ne gère le cas complexe que pour un seul chevauchement.
                if len(annual_overlaps) == 1:
                    annual = annual_overlaps[0]
                    # Cas 1: Remplacement Total
                    if start_date <= annual.date_debut and end_date >= annual.date_fin:
                        if messagebox.askyesno("Confirmation", "Ce congé va remplacer entièrement un congé annuel. Continuer ?"):
                            return self._handle_total_replacement(annual, form_data)
                    # Cas 2: Division
                    elif start_date > annual.date_debut and end_date < annual.date_fin:
                        if messagebox.askyesno("Confirmation", "Ce congé va diviser un congé annuel. Continuer ?"):
                            return self._handle_division(annual, form_data)
                    # Cas 3: Rognage par la fin
                    elif start_date > annual.date_debut and end_date >= annual.date_fin:
                         if messagebox.askyesno("Confirmation", "Ce congé va raccourcir un congé annuel. Continuer ?"):
                            return self._handle_trim_from_start(annual, form_data)
                    # Cas 4: Rognage par le début
                    elif start_date <= annual.date_debut and end_date < annual.date_fin:
                        if messagebox.askyesno("Confirmation", "Ce congé va raccourcir un congé annuel. Continuer ?"):
                            return self._handle_trim_from_end(annual, form_data)
                    else:
                        raise ValueError("Type de chevauchement non géré.")
                # Si plusieurs congés sont chevauchés, on les remplace tous.
                else:
                    if messagebox.askyesno("Confirmation", f"Ce congé va remplacer {len(annual_overlaps)} congés annuels. Continuer ?"):
                        return self._handle_total_replacement(annual_overlaps, form_data)
                return False

            # S'il n'y a pas de chevauchement, on ajoute normalement
            conge_model = Conge(id=None, agent_id=form_data['agent_id'], type_conge=form_data['type_conge'],
                                justif=form_data.get('justif'), interim_id=form_data.get('interim_id'), 
                                date_debut=start_date.strftime('%Y-%m-%d'), date_fin=end_date.strftime('%Y-%m-%d'), 
                                jours_pris=form_data['jours_pris'])
            
            conge_id = self.db.ajouter_conge(conge_model)
            if conge_id and form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, False, conge_id)
            return True if conge_id else False

        except (ValueError, sqlite3.Error) as e:
            messagebox.showerror("Erreur de validation", str(e)); return False
        except Exception as e:
            logging.error(f"Erreur soumission congé: {e}", exc_info=True)
            messagebox.showerror("Erreur Inattendue", str(e)); return False

    def _add_new_leave_from_form(self, cursor, form_data):
        """Helper pour ajouter le nouveau congé et gérer le certificat."""
        new_start = validate_date(form_data['date_debut'])
        new_end = validate_date(form_data['date_fin'])
        new_conge_model = Conge(None, form_data['agent_id'], form_data['type_conge'], form_data.get('justif'), 
                                form_data.get('interim_id'), new_start.strftime('%Y-%m-%d'), 
                                new_end.strftime('%Y-%m-%d'), form_data['jours_pris'])
        new_conge_id = self.db._ajouter_conge_no_commit(cursor, new_conge_model)
        if new_conge_id and form_data['type_conge'] == "Congé de maladie":
            self._handle_certificat_save(form_data, False, new_conge_id)

    def _handle_division(self, annual, form_data):
        """Cas 1: Division (maladie au milieu)"""
        try:
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            new_start = validate_date(form_data['date_debut'])
            new_end = validate_date(form_data['date_fin'])
            holidays_set = get_holidays_set_for_period(self.db, new_start.year - 1, new_end.year + 2)

            cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (annual.id,))
            cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (annual.jours_pris, annual.agent_id))
            
            self._creer_segment(cursor, annual.agent_id, annual.date_debut, new_start - timedelta(days=1), holidays_set)
            self._creer_segment(cursor, annual.agent_id, new_end + timedelta(days=1), annual.date_fin, holidays_set)
            self._add_new_leave_from_form(cursor, form_data)
            
            self.db.conn.commit(); return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback(); raise e

    def _handle_total_replacement(self, annuals, form_data):
        """Cas 2: Remplacement total"""
        try:
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            annual_list = annuals if isinstance(annuals, list) else [annuals]
            for annual in annual_list:
                cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (annual.id,))
                cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (annual.jours_pris, annual.agent_id))
            
            self._add_new_leave_from_form(cursor, form_data)

            self.db.conn.commit(); return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback(); raise e

    def _handle_trim_from_start(self, annual, form_data):
        """Cas 3: Rognage par la fin (la maladie commence PENDANT)"""
        try:
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            new_start = validate_date(form_data['date_debut'])
            holidays_set = get_holidays_set_for_period(self.db, new_start.year - 1, new_start.year + 2)

            cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (annual.id,))
            cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (annual.jours_pris, annual.agent_id))

            self._creer_segment(cursor, annual.agent_id, annual.date_debut, new_start - timedelta(days=1), holidays_set)
            self._add_new_leave_from_form(cursor, form_data)

            self.db.conn.commit(); return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback(); raise e

    def _handle_trim_from_end(self, annual, form_data):
        """Cas 4: Rognage par le début (la maladie finit PENDANT)"""
        try:
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            new_end = validate_date(form_data['date_fin'])
            holidays_set = get_holidays_set_for_period(self.db, new_end.year - 1, new_end.year + 2)

            cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (annual.id,))
            cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (annual.jours_pris, annual.agent_id))

            self._creer_segment(cursor, annual.agent_id, new_end + timedelta(days=1), annual.date_fin, holidays_set)
            self._add_new_leave_from_form(cursor, form_data)

            self.db.conn.commit(); return True
        except (sqlite3.Error, ValueError) as e:
            self.db.conn.rollback(); raise e

    def _creer_segment(self, cursor, agent_id, date_debut, date_fin, holidays_set):
        """Helper pour créer un segment de congé annuel ET déduire ses jours du solde."""
        if date_debut > date_fin: return
        jours = jours_ouvres(date_debut, date_fin, holidays_set)
        if jours > 0:
            segment = Conge(None, agent_id, 'Congé annuel', None, None, date_debut.strftime('%Y-%m-%d'), date_fin.strftime('%Y-%m-%d'), jours)
            self.db._ajouter_conge_no_commit(cursor, segment)

    def _handle_certificat_save(self, form_data, is_modification, conge_id):
        new_path = form_data.get('cert_path')
        original_path = form_data.get('original_cert_path')
        if not new_path or not conge_id: return
        if os.path.exists(new_path) and new_path != original_path:
            try:
                filename = f"cert_{form_data['agent_ppr']}_{conge_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{os.path.splitext(new_path)[1]}"
                dest_path = os.path.join(self.certificats_dir, filename)
                shutil.copy(new_path, dest_path)
                cert_model = type('Certificat', (object,), {'duree_jours': form_data['jours_pris'], 'chemin_fichier': dest_path})()
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