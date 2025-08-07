# core/conges/manager.py

import sqlite3
from tkinter import messagebox
import logging
import os
import shutil
from datetime import datetime, timedelta

# Importations des utilitaires et modèles nécessaires
from utils.date_utils import get_holidays_set_for_period, jours_ouvres, validate_date
from utils.config_loader import CONFIG
from db.models import Agent, Conge


class CongeManager:
    """
    Orchestre toute la logique métier liée à la gestion des agents et des congés.
    C'est le "cerveau" de l'application.
    """
    def __init__(self, db_manager, certificats_dir):
        self.db = db_manager
        self.certificats_dir = certificats_dir

    # --- Logique Agent ---
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

    # --- Logique Congé ---
    def get_conges_for_agent(self, agent_id):
        return self.db.get_conges(agent_id=agent_id)
        
    def get_conge_by_id(self, conge_id):
        return self.db.get_conge_by_id(conge_id)

    def delete_conge_with_confirmation(self, conge_id):
        if messagebox.askyesno("Confirmation", "Êtes-vous sûr de vouloir supprimer ce congé ?\nLe solde de l'agent sera ajusté si applicable."):
            return self.db.supprimer_conge(conge_id)
        return False

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
                    else:
                        return False # L'utilisateur a refusé le remplacement
                else:
                    raise ValueError("Les dates sélectionnées chevauchent un congé déjà existant qui ne peut pas être remplacé.")
            
            # S'il n'y a pas de chevauchement, on exécute la logique d'ajout/modification standard.
            conge_model = Conge(
                id=form_data.get('conge_id'), agent_id=form_data['agent_id'],
                type_conge=form_data['type_conge'], justif=form_data.get('justif'),
                interim_id=form_data.get('interim_id'), date_debut=start_date.strftime('%Y-%m-%d'),
                date_fin=end_date.strftime('%Y-%m-%d'), jours_pris=form_data['jours_pris']
            )
            
            conge_id = None
            if is_modification:
                conge_id = self.db.modifier_conge(conge_model.id, conge_model)
            else:
                conge_id = self.db.ajouter_conge(conge_model)

            if conge_id and form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, is_modification, conge_id)

            return True if conge_id else False

        except (ValueError, sqlite3.Error) as e:
            messagebox.showerror("Erreur de validation", str(e))
            return False
        except Exception as e:
            logging.error(f"Erreur inattendue lors de la soumission du congé: {e}", exc_info=True)
            messagebox.showerror("Erreur Inattendue", str(e))
            return False

    def _handle_certificat_save(self, form_data, is_modification, conge_id):
        new_path = form_data['cert_path']
        original_path = form_data['original_cert_path']
        valid_conge_id = conge_id or form_data.get('conge_id')

        if new_path and os.path.exists(new_path) and new_path != original_path:
            try:
                filename = f"cert_{form_data['agent_ppr']}_{valid_conge_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{os.path.splitext(new_path)[1]}"
                dest_path = os.path.join(self.certificats_dir, filename)
                shutil.copy(new_path, dest_path)
                
                certificat_model = type('Certificat', (object,), {
                    'nom_medecin': "", 'duree_jours': form_data['jours_pris'], 'chemin_fichier': dest_path
                })()
                cursor = self.db.conn.cursor()
                self.db._add_or_update_certificat_no_commit(cursor, valid_conge_id, certificat_model)
                self.db.conn.commit()

                if original_path and os.path.exists(original_path):
                    os.remove(original_path)
            except Exception as e:
                self.db.conn.rollback()
                logging.error(f"Erreur sauvegarde certificat: {e}", exc_info=True)
                messagebox.showwarning("Erreur Certificat", f"Le congé a été sauvegardé, mais le fichier certificat n'a pas pu être copié:\n{e}")
        
        elif not new_path and original_path:
            try:
                self.db.execute_query("DELETE FROM certificats_medicaux WHERE conge_id = ?", (valid_conge_id,))
                if os.path.exists(original_path):
                    os.remove(original_path)
            except Exception as e:
                logging.error(f"Impossible de supprimer l'ancien certificat pour conge_id {conge_id}: {e}")

    def split_and_replace_annual_leave(self, annual_overlap, form_data):
        agent_id = form_data['agent_id']
        
        logging.info(f"Début du processus de division/remplacement pour l'agent ID {agent_id}.")
        try:
            old_conge_id = annual_overlap.id
            old_start = annual_overlap.date_debut
            old_end = annual_overlap.date_fin
            old_days_deducted = annual_overlap.jours_pris
            
            new_start = validate_date(form_data['date_debut'])
            new_end = validate_date(form_data['date_fin'])

            holidays_set = get_holidays_set_for_period(self.db, old_start.year, new_end.year + 2)
            
            days_for_segment1 = jours_ouvres(old_start, new_start - timedelta(days=1), holidays_set) if old_start < new_start else 0
            days_for_segment2 = jours_ouvres(new_end + timedelta(days=1), old_end, holidays_set) if old_end > new_end else 0

            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()

            cursor.execute("UPDATE conges SET statut = 'Annulé' WHERE id=?", (old_conge_id,))
            
            if annual_overlap.type_conge in CONFIG['conges']['types_decompte_solde']:
                cursor.execute("UPDATE agents SET solde = solde + ? WHERE id=?", (old_days_deducted, agent_id))

            new_conge_model = Conge(
                id=None, agent_id=agent_id, type_conge=form_data['type_conge'],
                justif=form_data['justif'], interim_id=form_data['interim_id'],
                date_debut=new_start.strftime('%Y-%m-%d'), date_fin=new_end.strftime('%Y-%m-%d'),
                jours_pris=form_data['jours_pris']
            )
            new_conge_id = self.db._ajouter_conge_no_commit(cursor, new_conge_model)

            if form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, False, new_conge_id)

            if days_for_segment1 > 0:
                segment1_model = Conge(
                    id=None, agent_id=agent_id, type_conge='Congé annuel', justif=None,
                    interim_id=None, date_debut=old_start.strftime('%Y-%m-%d'),
                    date_fin=(new_start - timedelta(days=1)).strftime('%Y-%m-%d'), jours_pris=int(days_for_segment1)
                )
                self.db._ajouter_conge_no_commit(cursor, segment1_model)
            
            if days_for_segment2 > 0:
                segment2_model = Conge(
                    id=None, agent_id=agent_id, type_conge='Congé annuel', justif=None,
                    interim_id=None, date_debut=(new_end + timedelta(days=1)).strftime('%Y-%m-%d'),
                    date_fin=old_end.strftime('%Y-%m-%d'), jours_pris=int(days_for_segment2)
                )
                self.db._ajouter_conge_no_commit(cursor, segment2_model)
            
            self.db.conn.commit()
            logging.info(f"Transaction terminée avec succès. Congé {old_conge_id} annulé et remplacé.")
            return True

        except (sqlite3.Error, OSError, ValueError) as e:
            self.db.conn.rollback()
            logging.error(f"Échec de la transaction split_and_replace_annual_leave: {e}", exc_info=True)
            raise e