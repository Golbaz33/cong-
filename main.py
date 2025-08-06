# main.py
import tkinter as tk
from tkinter import messagebox
import sys
import os
import logging

# --- Étape 1 : Définir les chemins de base ---
# C'est la clé pour que l'application trouve ses fichiers, peu importe d'où elle est lancée.
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd() # Solution de secours si __file__ n'est pas défini

CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

# --- Étape 2 : Charger la configuration AVANT tout le reste ---
# C'est crucial car tous les autres modules dépendent de CONFIG.
try:
    from utils.config_loader import load_config, CONFIG
    load_config(CONFIG_PATH)
except FileNotFoundError as e:
    root = tk.Tk(); root.withdraw()
    messagebox.showerror("Erreur Critique", f"Fichier de configuration introuvable:\n{e}")
    sys.exit(1)
except ImportError:
    root = tk.Tk(); root.withdraw()
    messagebox.showerror("Erreur de Structure", "Le fichier 'utils/config_loader.py' est introuvable ou corrompu.")
    sys.exit(1)


# --- Étape 3 : Importer les autres composants de l'architecture ---
# On ne peut le faire qu'après le chargement de la configuration.
from db.database import DatabaseManager
from core.conges.manager import CongeManager
from ui.main_window import MainWindow


if __name__ == "__main__":
    # --- Étape 4 : Vérifier les dépendances externes ---
    try:
        import tkcalendar
        import dateutil
        import holidays
        import yaml
        import openpyxl
    except ImportError as e:
        root = tk.Tk(); root.withdraw()
        messagebox.showerror("Bibliothèque Manquante", f"Une bibliothèque nécessaire est manquante : {e.name}.\n\nVeuillez l'installer en ouvrant un terminal et en tapant :\npip install {e.name}")
        sys.exit(1)

    # --- Étape 5 : Préparer l'environnement ---
    CERTIFICATS_DIR_ABS = os.path.join(BASE_DIR, CONFIG['db']['certificates_dir'])
    if not os.path.exists(CERTIFICATS_DIR_ABS):
        os.makedirs(CERTIFICATS_DIR_ABS)
        
    DB_PATH_ABS = os.path.join(BASE_DIR, CONFIG['db']['filename'])
    
    # Configuration du logging (le fichier log sera aussi à la racine du projet)
    LOG_FILE_PATH = os.path.join(BASE_DIR, "conges.log")
    logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # --- Étape 6 : Initialiser les composants principaux dans le bon ordre ---
    
    # 6.1. Créer le gestionnaire de base de données
    db_manager = DatabaseManager(DB_PATH_ABS)
    
    # 6.2. Tenter la connexion à la base de données
    if not db_manager.connect():
        # Si la connexion échoue, un message d'erreur est déjà affiché. On arrête.
        sys.exit(1)
        
    # 6.3. S'assurer que les tables existent
    db_manager.create_db_tables()
    
    # 6.4. Créer le "cerveau" de l'application
    conge_manager = CongeManager(db_manager, CERTIFICATS_DIR_ABS)
    
    # 6.5. Créer et lancer la fenêtre principale
    print(f"--- Lancement de {CONFIG['app']['title']} v{CONFIG['app']['version']} ---")
    app = MainWindow(conge_manager)
    app.mainloop()