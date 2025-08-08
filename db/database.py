import sqlite3
from tkinter import messagebox
import logging
import os

from db.models import Agent, Conge
try:
    from utils.config_loader import CONFIG
except ImportError:
    CONFIG = {'conges': {'types_decompte_solde': ['Congé annuel']}}

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.conn.execute("PRAGMA foreign_keys = ON")
            return True
        except sqlite3.Error as e:
            messagebox.showerror("Erreur Base de Données", f"Impossible de se connecter : {e}")
            return False

    def close(self):
        if self.conn: self.conn.close()

    def execute_query(self, query, params=(), fetch=None):
        if not self.conn:
            raise sqlite3.Error("Pas de connexion à la base de données.")
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            if fetch == "one": return cursor.fetchone()
            if fetch == "all": return cursor.fetchall()
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            logging.error(f"Erreur SQL: {query} avec params {params} -> {e}", exc_info=True)
            raise e

    def create_db_tables(self):
        try:
            self.execute_query("""CREATE TABLE IF NOT EXISTS agents (id INTEGER PRIMARY KEY, nom TEXT NOT NULL, prenom TEXT, ppr TEXT UNIQUE NOT NULL, grade TEXT NOT NULL, solde REAL NOT NULL CHECK(solde >= 0))""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS conges (id INTEGER PRIMARY KEY, agent_id INTEGER NOT NULL, type_conge TEXT NOT NULL, justif TEXT, interim_id INTEGER, date_debut TEXT NOT NULL, date_fin TEXT NOT NULL, jours_pris INTEGER NOT NULL CHECK(jours_pris >= 0), statut TEXT NOT NULL DEFAULT 'Actif', FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE, FOREIGN KEY (interim_id) REFERENCES agents(id) ON DELETE SET NULL)""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS jours_feries_personnalises (date TEXT PRIMARY KEY, nom TEXT NOT NULL, type TEXT NOT NULL)""")
            self.execute_query("""CREATE TABLE IF NOT EXISTS certificats_medicaux (id INTEGER PRIMARY KEY, conge_id INTEGER NOT NULL UNIQUE, nom_medecin TEXT, duree_jours INTEGER, chemin_fichier TEXT NOT NULL, FOREIGN KEY (conge_id) REFERENCES conges(id) ON DELETE CASCADE)""")
        except sqlite3.Error as e:
            messagebox.showerror("Erreur BD", f"Erreur création des tables : {e}")

    def _ajouter_conge_no_commit(self, cursor, conge_model):
        if conge_model.type_conge in CONFIG['conges']['types_decompte_solde']:
            agent_data = cursor.execute("SELECT solde FROM agents WHERE id=?", (conge_model.agent_id,)).fetchone()
            if agent_data[0] < conge_model.jours_pris:
                raise sqlite3.Error(f"Solde insuffisant ({agent_data[0]:.1f}j) pour décompter {conge_model.jours_pris}j.")
            cursor.execute("UPDATE agents SET solde = solde - ? WHERE id = ?", (conge_model.jours_pris, conge_model.agent_id))
        
        cursor.execute("INSERT INTO conges (agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (conge_model.agent_id, conge_model.type_conge, conge_model.justif, conge_model.interim_id, conge_model.date_debut, conge_model.date_fin, conge_model.jours_pris))
        return cursor.lastrowid

    def _supprimer_conge_no_commit(self, cursor, conge_id):
        conge = cursor.execute("SELECT agent_id, type_conge, jours_pris, statut FROM conges WHERE id=?", (conge_id,)).fetchone()
        if not conge: return
        agent_id, type_conge, jours_pris, statut = conge
        
        if type_conge in CONFIG['conges']['types_decompte_solde'] and statut == 'Actif':
            cursor.execute("UPDATE agents SET solde = solde + ? WHERE id = ?", (jours_pris, agent_id))
            
        cert = cursor.execute("SELECT chemin_fichier FROM certificats_medicaux WHERE conge_id = ?", (conge_id,)).fetchone()
        if cert and cert[0] and os.path.exists(cert[0]):
            try: os.remove(cert[0])
            except OSError as e: logging.error(f"Erreur suppression certificat pour conge_id {conge_id}: {e}")
        
        cursor.execute("DELETE FROM conges WHERE id=?", (conge_id,))

    def _add_or_update_certificat_no_commit(self, cursor, conge_id, cert_model):
        exists = cursor.execute("SELECT id FROM certificats_medicaux WHERE conge_id=?", (conge_id,)).fetchone()
        if exists: cursor.execute("UPDATE certificats_medicaux SET nom_medecin=?, duree_jours=?, chemin_fichier=? WHERE conge_id=?", (cert_model.nom_medecin, cert_model.duree_jours, cert_model.chemin_fichier, conge_id))
        else: cursor.execute("INSERT INTO certificats_medicaux (conge_id, nom_medecin, duree_jours, chemin_fichier) VALUES (?, ?, ?, ?)", (conge_id, cert_model.nom_medecin, cert_model.duree_jours, cert_model.chemin_fichier))

    def ajouter_conge(self, conge_model, cert_model=None):
        try:
            cursor = self.conn.cursor()
            conge_id = self._ajouter_conge_no_commit(cursor, conge_model)
            if cert_model and cert_model.chemin_fichier: self._add_or_update_certificat_no_commit(cursor, conge_id, cert_model)
            self.conn.commit()
            return conge_id
        except sqlite3.Error as e: self.conn.rollback(); raise e

    def modifier_conge(self, old_conge_id, new_conge_model, cert_model=None):
        try:
            cursor = self.conn.cursor()
            self._supprimer_conge_no_commit(cursor, old_conge_id)
            new_conge_id = self._ajouter_conge_no_commit(cursor, new_conge_model)
            if cert_model and cert_model.chemin_fichier: self._add_or_update_certificat_no_commit(cursor, new_conge_id, cert_model)
            self.conn.commit()
            return new_conge_id
        except sqlite3.Error as e: self.conn.rollback(); raise e

    def supprimer_conge(self, conge_id):
        try:
            cursor = self.conn.cursor()
            self._supprimer_conge_no_commit(cursor, conge_id)
            self.conn.commit()
            return True
        except sqlite3.Error as e: self.conn.rollback(); raise e
    
    def get_agents(self, term=None, limit=None, offset=None, exclude_id=None):
        q = "SELECT id, nom, prenom, ppr, grade, solde FROM agents"
        p, c = [], []
        if term:
            t = f"%{term.lower()}%"
            c.append("(LOWER(nom) LIKE ? OR LOWER(prenom) LIKE ? OR LOWER(ppr) LIKE ?)")
            p.extend([t, t, t])
        if exclude_id is not None:
            c.append("id != ?"); p.append(exclude_id)
        if c: q += " WHERE " + " AND ".join(c)
        q += " ORDER BY nom, prenom"
        if limit is not None: q += " LIMIT ? OFFSET ?"; p.extend([limit, offset])
        return [Agent.from_db_row(r) for r in self.execute_query(q, tuple(p), fetch="all") if r]

    def get_agents_count(self, term=None):
        q, p = "SELECT COUNT(*) FROM agents", []
        if term:
            t = f"%{term.lower()}%"
            q += " WHERE LOWER(nom) LIKE ? OR LOWER(prenom) LIKE ? OR LOWER(ppr) LIKE ?"
            p.extend([t, t, t])
        return self.execute_query(q, tuple(p), fetch="one")[0]

    def get_agent_by_id(self, agent_id):
        r = self.execute_query("SELECT id, nom, prenom, ppr, grade, solde FROM agents WHERE id=?", (agent_id,), fetch="one")
        return Agent.from_db_row(r) if r else None
        
    def get_conges(self, agent_id=None):
        q, p = "SELECT id, agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris, statut FROM conges", ()
        if agent_id: q += " WHERE agent_id=? ORDER BY date_debut DESC"; p = (agent_id,)
        else: q += " ORDER BY date_debut DESC"
        return [Conge.from_db_row(r) for r in self.execute_query(q, p, fetch="all") if r]

    def get_conge_by_id(self, conge_id):
        r = self.execute_query("SELECT id, agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris, statut FROM conges WHERE id=?", (conge_id,), fetch="one")
        return Conge.from_db_row(r) if r else None

    def ajouter_agent(self, nom, prenom, ppr, grade, solde):
        try: self.execute_query("INSERT INTO agents (nom, prenom, ppr, grade, solde) VALUES (?, ?, ?, ?, ?)",(nom.strip(), prenom.strip(), ppr.strip(), grade.strip(), solde)); return True
        except sqlite3.IntegrityError: return False

    def modifier_agent(self, agent_id, nom, prenom, ppr, grade, solde):
        try: self.execute_query("UPDATE agents SET nom=?, prenom=?, ppr=?, grade=?, solde=? WHERE id=?",(nom.strip(), prenom.strip(), ppr.strip(), grade.strip(), solde, agent_id)); return True
        except sqlite3.IntegrityError: return False

    def supprimer_agent(self, agent_id):
        self.execute_query("DELETE FROM agents WHERE id=?", (agent_id,)); return True

    def get_holidays_for_year(self, year):
        return self.execute_query("SELECT date, nom, type FROM jours_feries_personnalises WHERE strftime('%Y', date) = ? ORDER BY date", (str(year),), fetch="all")
        
    def get_certificat_for_conge(self, conge_id):
        return self.execute_query("SELECT * FROM certificats_medicaux WHERE conge_id = ?", (conge_id,), fetch="one")

    def get_overlapping_leaves(self, agent_id, start_date, end_date, conge_id_exclu=None):
        q = "SELECT * FROM conges WHERE agent_id=? AND date_fin >= ? AND date_debut <= ? AND statut = 'Actif'"
        p = [agent_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        if conge_id_exclu: q += " AND id != ?"; p.append(conge_id_exclu)
        return [Conge.from_db_row(r) for r in self.execute_query(q, tuple(p), fetch="all") if r]
