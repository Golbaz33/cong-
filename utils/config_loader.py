# utils/config_loader.py
import yaml
import os
import sys
import tkinter as tk
from tkinter import messagebox

# On initialise une variable globale vide. Elle sera remplie par main.py.
CONFIG = {}

def load_config(path):
    """
    Charge la configuration depuis un chemin absolu et la stocke dans la variable globale CONFIG.
    """
    global CONFIG
    if not os.path.exists(path):
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Fichier de Configuration Manquant",
            f"Le fichier de configuration '{os.path.basename(path)}' est introuvable.\n"
            f"Il doit se trouver ici : {os.path.dirname(path)}"
        )
        sys.exit(1) 
        
    with open(path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
        CONFIG.update(config_data) # On remplit le dictionnaire global