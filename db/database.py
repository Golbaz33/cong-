# db/database.py

import sqlite3
from tkinter import messagebox
import logging
import os

# Import des modèles pour transformer les données brutes en objets
from db.models import Agent, Conge

# Import de la configuration globale, nécessaire pour les règles métier (ex: types de congés)
try:
    from utils.config_loader import CONFIG
except ImportError:
    # Solution de secours si le module est utilisé seul, bien que peu probable dans le projet
    print("AVERTISSEMENT: Impossible d'importer la configuration. Certaines logiques pourraient échouer.")
    CONFIG = {'conges': {'types_decompte_solde': ['Congé annuel']}}


class DatabaseManager:
    """
    Couche d'accès aux données (Data Access Layer).
    Toutes les interactions avec la base de données SQLite sont centralisées ici.
    Cette classe ne contient PAS de logique métier, seulement des opérations CRUD sur la base.
    """
    def __init__(self, db_file):
        """Initialise le gestionnaire avec le chemin vers le fichier de la base de données."""
        self.db_file = db_file
        self.conn = None

    def connect(self):
        """Établit la connexion à la base de données."""
        try:
            self.conn = sqlite3.connect(self.db_file)
            # Activer les contraintes de clé étrangère pour assurer l'intégrité des données
            self.conn.execute("PRAGMA foreign_keys = ON")
            return True
        except sqlite3.Error as e:
            messagebox.showerror("Erreur Base de Données", f"Impossible de se connecter à la base : {e}")
            return False

    def close(self):
        """Ferme la connexion à la base de données si elle est ouverte."""
        if self.conn:
            self.conn.close()

    def execute_query(self, query, params=(), fetch=None):
        """Méthode générique pour exécuter des requêtes SQL."""
        if not self.conn:
            logging.error("Exécution de requête annulée : pas de connexion.")
            raise sqlite3.Error("Pas de connexion à la base de données.")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            if fetch == "one":
                return cursor.fetchone()
            if fetch == "all":
                return cursor.fetchall()
            # Pour les requêtes INSERT, UPDATE, DELETE non transactionnelles
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            # Pour les requêtes simples, un rollback est une sécurité
            self.conn.rollback()
            logging.error(f"Erreur SQL: {query} avec params {params} -> {e}", exc_info=True)
            # Fait remonter l'erreur pour que la couche supérieure (manager) puisse la gérer
            raise e

    def create_db_tables(self):
        """Crée les tables de la base de données si elles n'existent pas."""
        try:
            self.execute_query("""CREATE TABLE IF NOT EXISTS agents (
                                  id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                  nom TEXT NOT NULL, 
                                  prenom TEXT, 
                                  ppr TEXT UNIQUE NOT NULL, 
                                  grade TEXT NOT NULL, 
                                  solde REAL NOT NULL CHECK(solde >= 0))""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS conges (
                                  id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                  agent_id INTEGER NOT NULL, 
                                  type_conge TEXT NOT NULL, 
                                  justif TEXT, 
                                  interim_id INTEGER, 
                                  date_debut TEXT NOT NULL, 
                                  date_fin TEXT NOT NULL, 
                                  jours_pris INTEGER NOT NULL CHECK(jours_pris >= 0), 
                                  FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE, 
                                  FOREIGN KEY (interim_id) REFERENCES agents(id) ON DELETE SET NULL)""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS jours_feries_personnalises (
                                  date TEXT PRIMARY KEY, 
                                  nom TEXT NOT NULL, 
                                  type TEXT NOT NULL)""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS certificats_medicaux (
                                  id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                  conge_id INTEGER NOT NULL UNIQUE, 
                                  nom_medecin TEXT, 
                                  duree_jours INTEGER, 
                                  chemin_fichier TEXT NOT NULL, 
                                  FOREIGN KEY (conge_id) REFERENCES conges(id) ON DELETE CASCADE)""")
        except sqlite3.Error as e:
            messagebox.showerror("Erreur BD", f"Erreur lors de la création des tables : {e}")

    # --- Méthodes privées pour les transactions ---

    def _ajouter_conge_no_commit(self, cursor, conge_model):
        types_decompte = CONFIG['conges']['types_decompte_solde']
        if conge_model.type_conge in types_decompte:
            agent_data = cursor.execute("SELECT solde FROM agents WHERE id=?", (conge_model.agent_id,)).fetchone()
            if agent_data[0] < conge_model.jours_pris:
                raise sqlite3.Error(f"Solde insuffisant ({agent_data[0]:.1f}j) pour décompter {conge_model.jours_pris}j.")
            cursor.execute("UPDATE agents SET solde = solde - ? WHERE id = ?", (conge_model.jours_pris, conge_model.agent_id))
        
        cursor.execute("INSERT INTO conges (agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (conge_model.agent_id, conge_model.type_conge, conge_model.justif, conge_model.interim_id, conge_model.date_debut, conge_model.date_fin, conge_model.jours_pris))
        return cursor.lastrowid

    def _supprimer_conge_no_commit(self, cursor, conge_id):
        conge = cursor.execute("SELECT agent_id, type_conge, jours_pris FROM conges WHERE id=?", (conge_id,)).fetchone()
        if not conge: return
        agent_id, type_conge, jours_pris = conge
        
        types_decompte = CONFIG['conges']['types_decompte_solde']
        if type_conge in types_decompte:
            cursor.execute("UPDATE agents SET solde = solde + ? WHERE id = ?", (jours_pris, agent_id))
            
        cert = cursor.execute("SELECT chemin_fichier FROM certificats_medicaux WHERE conge_id = ?", (conge_id,)).fetchone()
        if cert and cert[0] and os.path.exists(cert[0]):
            try:
                os.remove(cert[0])
            except OSError as e:
                logging.error(f"Erreur suppression fichier certificat pour conge_id {conge_id}: {e}")
        
        cursor.execute("DELETE FROM conges WHERE id=?", (conge_id,))

    def _add_or_update_certificat_no_commit(self, cursor, conge_id, certificat_model):
        exists = cursor.execute("SELECT id FROM certificats_medicaux WHERE conge_id = ?", (conge_id,)).fetchone()
        if exists:
            cursor.execute("UPDATE certificats_medicaux SET nom_medecin=?, duree_jours=?, chemin_fichier=? WHERE conge_id=?",
                           (certificat_model.nom_medecin, certificat_model.duree_jours, certificat_model.chemin_fichier, conge_id))
        else:
            cursor.execute("INSERT INTO certificats_medicaux (conge_id, nom_medecin, duree_jours, chemin_fichier) VALUES (?, ?, ?, ?)",
                           (conge_id, certificat_model.nom_medecin, certificat_model.duree_jours, certificat_model.chemin_fichier))

    # --- Méthodes publiques transactionnelles pour les Congés ---

    def ajouter_conge(self, conge_model, certificat_model=None):
        cursor = self.conn.cursor()
        try:
            conge_id = self._ajouter_conge_no_commit(cursor, conge_model)
            if certificat_model and certificat_model.chemin_fichier:
                self._add_or_update_certificat_no_commit(cursor, conge_id, certificat_model)
            self.conn.commit()
            logging.info(f"Congé ID {conge_id} ajouté avec succès pour l'agent ID {conge_model.agent_id}.")
            return conge_id
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Échec transaction ajout de congé pour agent {conge_model.agent_id}: {e}", exc_info=True)
            raise e

    def modifier_conge(self, old_conge_id, new_conge_model, certificat_model=None):
        cursor = self.conn.cursor()
        try:
            self._supprimer_conge_no_commit(cursor, old_conge_id)
            new_conge_id = self._ajouter_conge_no_commit(cursor, new_conge_model)
            if certificat_model and certificat_model.chemin_fichier:
                self._add_or_update_certificat_no_commit(cursor, new_conge_id, certificat_model)
            self.conn.commit()
            logging.info(f"Congé ID {old_conge_id} modifié avec succès en nouveau congé ID {new_conge_id}.")
            return new_conge_id
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Échec transaction modification de congé ID {old_conge_id}: {e}", exc_info=True)
            raise e

    def supprimer_conge(self, conge_id):
        cursor = self.conn.cursor()
        try:
            self._supprimer_conge_no_commit(cursor, conge_id)
            self.conn.commit()
            logging.info(f"Congé ID {conge_id} supprimé avec succès.")
            return True
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Échec transaction suppression de congé ID {conge_id}: {e}", exc_info=True)
            raise e
    
    # --- Méthodes de lecture et CRUD (Agents, Jours Fériés, etc.) ---
    
    def get_agents(self, term=None, limit=None, offset=None, exclude_id=None):
        query = "SELECT id, nom, prenom, ppr, grade, solde FROM agents"
        params = []
        conditions = []
        if term:
            term_like = f"%{term.lower()}%"
            conditions.append("(LOWER(nom) LIKE ? OR LOWER(prenom) LIKE ? OR LOWER(ppr) LIKE ?)")
            params.extend([term_like, term_like, term_like])
        if exclude_id is not None:
            conditions.append("id != ?")
            params.append(exclude_id)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY nom, prenom"
        if limit is not None and offset is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        
        rows = self.execute_query(query, tuple(params), fetch="all")
        return [Agent.from_db_row(row) for row in rows if row]

    def get_agents_count(self, term=None):
        query = "SELECT COUNT(*) FROM agents"
        params = []
        if term:
            term_like = f"%{term.lower()}%"
            query += " WHERE LOWER(nom) LIKE ? OR LOWER(prenom) LIKE ? OR LOWER(ppr) LIKE ?"
            params.extend([term_like, term_like, term_like])
        
        count = self.execute_query(query, tuple(params), fetch="one")[0]
        return count

    def get_agent_by_id(self, agent_id):
        row = self.execute_query("SELECT id, nom, prenom, ppr, grade, solde FROM agents WHERE id=?", (agent_id,), fetch="one")
        return Agent.from_db_row(row) if row else None
        
    def get_conges(self, agent_id=None):
        base_query = "SELECT id, agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris FROM conges"
        params = ()
        if agent_id:
            base_query += " WHERE agent_id=? ORDER BY date_debut DESC"
            params = (agent_id,)
        else:
            base_query += " ORDER BY date_debut DESC"
        
        rows = self.execute_query(base_query, params, fetch="all")
        return [Conge.from_db_row(row) for row in rows if row]

    def get_conge_by_id(self, conge_id):
        """
        Récupère un seul congé par son ID et le retourne comme un objet Conge.
        """
        query = "SELECT id, agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris FROM conges WHERE id = ?"
        row = self.execute_query(query, (conge_id,), fetch="one")
        return Conge.from_db_row(row) if row else None

    def ajouter_agent(self, nom, prenom, ppr, grade, solde):
        try:
            self.execute_query("INSERT INTO agents (nom, prenom, ppr, grade, solde) VALUES (?, ?, ?, ?, ?)",
                               (nom.strip(), prenom.strip(), ppr.strip(), grade.strip(), solde))
            return True
        except sqlite3.IntegrityError: # ppr déjà existant
            return False

    def modifier_agent(self, agent_id, nom, prenom, ppr, grade, solde):
        try:
            self.execute_query("UPDATE agents SET nom=?, prenom=?, ppr=?, grade=?, solde=? WHERE id=?",
                               (nom.strip(), prenom.strip(), ppr.strip(), grade.strip(), solde, agent_id))
            return True
        except sqlite3.IntegrityError:
            return False

    def supprimer_agent(self, agent_id):
        self.execute_query("DELETE FROM agents WHERE id=?", (agent_id,))
        return True

    def add_or_update_holiday(self, date_sql, name, holiday_type):
        self.execute_query("INSERT OR REPLACE INTO jours_feries_personnalises (date, nom, type) VALUES (?, ?, ?)", (date_sql, name, holiday_type))
        return True

    def delete_holiday(self, date_sql):
        self.execute_query("DELETE FROM jours_feries_personnalises WHERE date = ?", (date_sql,))
        return True
        
    def get_holidays_for_year(self, year):
        query = "SELECT date, nom, type FROM jours_feries_personnalises WHERE strftime('%Y', date) = ? ORDER BY date"
        return self.execute_query(query, (str(year),), fetch="all")
        
    def get_certificat_for_conge(self, conge_id):
        return self.execute_query("SELECT * FROM certificats_medicaux WHERE conge_id = ?", (conge_id,), fetch="one")

    def get_overlapping_leaves(self, agent_id, date_debut, date_fin, conge_id_exclu=None):
        query = "SELECT * FROM conges WHERE agent_id=? AND date_fin >= ? AND date_debut <= ?"
        params = [agent_id, date_debut.strftime('%Y-%m-%d'), date_fin.strftime('%Y-%m-%d')]
        if conge_id_exclu:
            query += " AND id != ?"
            params.append(conge_id_exclu)
        rows = self.execute_query(query, tuple(params), fetch="all")
        return [Conge.from_db_row(row) for row in rows if row]