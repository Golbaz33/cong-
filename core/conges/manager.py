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
from db.models import Agent, Conge # Conge est nécessaire pour la correction


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
            
            success = False
            conge_id = form_data.get('conge_id')

            if not overlaps:
                # ==================== DÉBUT DE LA CORRECTION ====================
                # Le problème venait du fait qu'on passait un dictionnaire (**form_data)
                # à une fonction qui attend maintenant des objets "modèles".
                
                # 1. On transforme le dictionnaire du formulaire en un objet Conge.
                conge_model = Conge(
                    id=form_data.get('conge_id'),
                    agent_id=form_data['agent_id'],
                    type_conge=form_data['type_conge'],
                    justif=form_data.get('justif'),
                    interim_id=form_data.get('interim_id'),
                    date_debut=start_date.strftime('%Y-%m-%d'),
                    date_fin=end_date.strftime('%Y-%m-%d'),
                    jours_pris=form_data['jours_pris']
                )

                # 2. On appelle les méthodes de la base de données avec les bons arguments.
                if is_modification:
                    # La méthode modifier_conge attend l'ancien ID et le nouvel objet.
                    conge_id = self.db.modifier_conge(conge_model.id, conge_model)
                else:
                    # La méthode ajouter_conge attend juste le nouvel objet.
                    conge_id = self.db.ajouter_conge(conge_model)
                
                success = True # Si les méthodes ci-dessus échouent, elles lèvent une exception.
                # ===================== FIN DE LA CORRECTION =====================

            else:
                annual_overlap = next((c for c in overlaps if c.type_conge == 'Congé annuel'), None)
                if form_data['type_conge'] != 'Congé annuel' and annual_overlap and len(overlaps) == 1:
                    if messagebox.askyesno("Confirmation de Remplacement", "Ce congé chevauche un congé annuel. Voulez-vous le remplacer ?"):
                        success = self.split_and_replace_annual_leave(annual_overlap, form_data)
                else:
                    raise ValueError("Les dates sélectionnées chevauchent un congé déjà existant qui ne peut pas être remplacé automatiquement.")
            
            # La logique de gestion du certificat reste inchangée, comme vous l'avez demandé.
            if success and form_data['type_conge'] == "Congé de maladie":
                 self._handle_certificat_save(form_data, is_modification, conge_id)

            return success

        except (ValueError, sqlite3.Error) as e:
            messagebox.showerror("Erreur de validation", str(e))
            return False
        except Exception as e:
            logging.error(f"Erreur inattendue lors de la soumission du congé: {e}", exc_info=True)
            messagebox.showerror("Erreur Inattendue", str(e))
            return False

    def _handle_certificat_save(self, form_data, is_modification, conge_id):
        # Cette méthode est conservée telle quelle, sans la refactorisation.
        new_path = form_data['cert_path']
        original_path = form_data['original_cert_path']

        if new_path and os.path.exists(new_path) and new_path != original_path:
            try:
                filename = f"cert_{form_data['agent_ppr']}_{conge_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{os.path.splitext(new_path)[1]}"
                dest_path = os.path.join(self.certificats_dir, filename)
                shutil.copy(new_path, dest_path)
                
                self.db.conn.execute('BEGIN TRANSACTION')
                cursor = self.db.conn.cursor()
                self.db._add_or_update_certificat_no_commit(cursor, conge_id, "", form_data['jours_pris'], dest_path)
                self.db.conn.commit()

                if original_path and os.path.exists(original_path):
                    os.remove(original_path)
            except Exception as e:
                self.db.conn.rollback()
                logging.error(f"Erreur sauvegarde certificat: {e}", exc_info=True)
                messagebox.showwarning("Erreur Certificat", f"Le congé a été sauvegardé, mais le fichier certificat n'a pas pu être copié:\n{e}")
        
        elif not new_path and original_path:
            try:
                self.db.execute_query("DELETE FROM certificats_medicaux WHERE conge_id = ?", (conge_id,))
                if os.path.exists(original_path):
                    os.remove(original_path)
            except Exception as e:
                logging.error(f"Impossible de supprimer l'ancien certificat pour conge_id {conge_id}: {e}")

    def split_and_replace_annual_leave(self, annual_overlap, form_data):
        # Cette méthode est également conservée telle quelle.
        agent_id = form_data['agent_id']
        agent_ppr = form_data['agent_ppr']
        
        logging.info(f"Début du processus de division/remplacement pour l'agent ID {agent_id}.")
        try:
            old_conge_id = annual_overlap[0]
            old_start = validate_date(annual_overlap[5])
            old_end = validate_date(annual_overlap[6])
            old_days_deducted = int(annual_overlap[7])
            
            new_start = validate_date(form_data['date_debut'])
            new_end = validate_date(form_data['date_fin'])

            logging.debug(f"Dates parsées - Ancien: {old_start.strftime('%Y-%m-%d')} -> {old_end.strftime('%Y-%m-%d')}, Nouveau: {new_start.strftime('%Y-%m-%d')} -> {new_end.strftime('%Y-%m-%d')}")

            holidays_set = get_holidays_set_for_period(self.db, old_start.year, new_end.year + 2)
            
            days_for_segment1 = 0
            if old_start < new_start:
                segment1_end_date = new_start - timedelta(days=1)
                days_for_segment1 = jours_ouvres(old_start, segment1_end_date, holidays_set)
            
            days_for_segment2 = 0
            if old_end > new_end:
                segment2_start_date = new_end + timedelta(days=1)
                days_for_segment2 = jours_ouvres(segment2_start_date, old_end, holidays_set)

            if days_for_segment1 < 0 or days_for_segment2 < 0:
                raise ValueError("Calcul de segment de congé invalide (jours négatifs).")

            new_annual_cost = days_for_segment1 + days_for_segment2
            
            agent = self.db.get_agent_by_id(agent_id)
            current_solde = agent.solde
            
            solde_intermediaire = current_solde + old_days_deducted
            solde_final_projected = solde_intermediaire - new_annual_cost

            logging.info(f"Solde actuel: {current_solde:.1f}j. Remboursement: +{old_days_deducted:.1f}j -> Solde intermédiaire: {solde_intermediaire:.1f}j.")
            logging.info(f"Nouveau coût: -{new_annual_cost:.1f}j -> Solde final projeté: {solde_final_projected:.1f}j.")

            if solde_final_projected < 0:
                error_message = (
                    f"L'opération échouerait car le solde final serait négatif.\n\n"
                    f"Détail du calcul :\n"
                    f"  Solde actuel de l'agent : {current_solde:.1f} j\n"
                    f"  Remboursement du congé initial : + {old_days_deducted:.1f} j\n"
                    f"  --------------------------------------------------\n"
                    f"  Solde théorique après remboursement : {solde_intermediaire:.1f} j\n"
                    f"  Nouveau coût du congé annuel restant : - {new_annual_cost:.1f} j\n"
                    f"  --------------------------------------------------\n"
                    f"  Solde final calculé : {solde_final_projected:.1f} j"
                )
                raise ValueError(error_message)
            
            self.db.conn.execute('BEGIN TRANSACTION')
            cursor = self.db.conn.cursor()
            cursor.execute("DELETE FROM conges WHERE id=?", (old_conge_id,))
            new_conge_id = cursor.execute(
                "INSERT INTO conges (agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (agent_id, form_data['type_conge'], form_data['justif'], form_data['interim_id'], new_start.strftime('%Y-%m-%d'), new_end.strftime('%Y-%m-%d'), form_data['jours_pris'])
            ).lastrowid
            
            if form_data['type_conge'] == "Congé de maladie" and form_data['cert_path'] and os.path.exists(form_data['cert_path']):
                self._handle_certificat_save(agent_ppr, new_conge_id, form_data['jours_pris'], form_data['cert_path'], cursor_for_transaction=cursor)

            if days_for_segment1 > 0:
                segment1_end_date = new_start - timedelta(days=1)
                cursor.execute("INSERT INTO conges (agent_id, type_conge, date_debut, date_fin, jours_pris) VALUES (?, ?, ?, ?, ?)",
                               (agent_id, 'Congé annuel', old_start.strftime('%Y-%m-%d'), segment1_end_date.strftime('%Y-%m-%d'), int(days_for_segment1)))
            
            if days_for_segment2 > 0:
                segment2_start_date = new_end + timedelta(days=1)
                cursor.execute("INSERT INTO conges (agent_id, type_conge, date_debut, date_fin, jours_pris) VALUES (?, ?, ?, ?, ?)",
                               (agent_id, 'Congé annuel', segment2_start_date.strftime('%Y-%m-%d'), old_end.strftime('%Y-%m-%d'), int(days_for_segment2)))
            
            cursor.execute("UPDATE agents SET solde = ? WHERE id = ?", (solde_final_projected, agent_id))
            
            self.db.conn.commit()
            logging.info(f"Transaction terminée avec succès. Congé {old_conge_id} remplacé.")
            return True

        except (sqlite3.Error, OSError, ValueError) as e:
            self.db.conn.rollback()
            logging.error(f"Échec de la transaction split_and_replace_annual_leave: {e}", exc_info=True)
            raise e