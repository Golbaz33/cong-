# ui/widgets/secondary_windows.py
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import holidays

# Import des composants nécessaires
from ui.widgets.date_picker import DatePickerWindow
from utils.date_utils import validate_date, format_date_for_display

class HolidaysManagerWindow(tk.Toplevel):
    """
    Fenêtre Toplevel pour l'ajout, la modification et la suppression
    des jours fériés personnalisés.
    """
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.db = db_manager
        
        self.title("Gestion des Jours Fériés")
        self.grab_set()
        self.resizable(False, False)

        self._create_widgets()
        self.refresh_holidays_list()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill="both", expand=True)

        # --- Cadre du haut : Liste des jours fériés ---
        top_frame = ttk.LabelFrame(main_frame, text="Jours Fériés Enregistrés")
        top_frame.pack(fill="x", expand=True, pady=5, padx=5)

        year_frame = ttk.Frame(top_frame, padding=5)
        year_frame.pack(fill="x")
        ttk.Label(year_frame, text="Année:").pack(side="left")
        
        current_year = datetime.now().year
        self.year_var = tk.StringVar(value=str(current_year))
        year_spinbox = ttk.Spinbox(
            year_frame, from_=current_year - 5, to=current_year + 5,
            textvariable=self.year_var, width=8, command=self.refresh_holidays_list
        )
        year_spinbox.pack(side="left", padx=5)
        ttk.Button(year_frame, text="Restaurer les jours automatiques", command=self.restore_auto_holidays).pack(side="right")

        cols = ("Date", "Description", "Type")
        self.holidays_tree = ttk.Treeview(top_frame, columns=cols, show="headings", height=10)
        self.holidays_tree.column("Date", width=100, anchor="center")
        self.holidays_tree.heading("Date", text="Date")
        self.holidays_tree.column("Description", width=250)
        self.holidays_tree.heading("Description", text="Description")
        self.holidays_tree.column("Type", width=100, anchor="center")
        self.holidays_tree.heading("Type", text="Type")
        self.holidays_tree.pack(fill="x", expand=True, padx=5, pady=5)
        self.holidays_tree.bind("<<TreeviewSelect>>", self._on_holiday_select)

        btn_frame = ttk.Frame(top_frame)
        btn_frame.pack(padx=5, pady=(0, 5), fill="x")
        self.modify_button = ttk.Button(btn_frame, text="Modifier", command=self.modify_selected_holiday, state="disabled")
        self.modify_button.pack(side="left", expand=True, fill="x", padx=2)
        self.delete_button = ttk.Button(btn_frame, text="Supprimer", command=self.delete_selected_holiday, state="disabled")
        self.delete_button.pack(side="left", expand=True, fill="x", padx=2)

        # --- Cadre du bas : Ajout d'un jour férié ---
        bottom_frame = ttk.LabelFrame(main_frame, text="Ajouter un Jour Férié Personnalisé")
        bottom_frame.pack(fill="x", expand=True, pady=5, padx=5)

        add_frame = ttk.Frame(bottom_frame, padding=5)
        add_frame.pack()
        ttk.Label(add_frame, text="Date:").grid(row=0, column=0, sticky="w", pady=2)
        self.date_entry = ttk.Entry(add_frame, width=15)
        self.date_entry.grid(row=0, column=1, padx=5)
        ttk.Button(add_frame, text="📅", width=2, command=lambda: DatePickerWindow(self, self.date_entry, self.db)).grid(row=0, column=2)
        
        ttk.Label(add_frame, text="Description:").grid(row=1, column=0, sticky="w", pady=2)
        self.desc_entry = ttk.Entry(add_frame, width=30)
        self.desc_entry.grid(row=1, column=1, columnspan=2, padx=5)
        
        ttk.Button(bottom_frame, text="Ajouter ce jour férié", command=self.add_holiday).pack(pady=5)

    def refresh_holidays_list(self):
        for row in self.holidays_tree.get_children():
            self.holidays_tree.delete(row)
        try:
            year = int(self.year_var.get())
            # On s'assure que les jours fériés officiels sont dans la DB
            auto_holidays = holidays.country_holidays('MA', years=year)
            for date_obj, name in auto_holidays.items():
                self.db.add_or_update_holiday(date_obj.strftime("%Y-%m-%d"), name, "Automatique")
            
            # On affiche tous les jours (officiels et perso)
            all_holidays = self.db.get_holidays_for_year(str(year))
            for h_date, h_name, h_type in all_holidays:
                self.holidays_tree.insert("", "end", values=(format_date_for_display(h_date), h_name, h_type))
        except (tk.TclError, ValueError):
            pass # Ignorer les erreurs si l'année n'est pas un entier valide
        except sqlite3.Error as e:
            messagebox.showerror("Erreur BD", f"Impossible de charger les jours fériés: {e}", parent=self)
        self._on_holiday_select()

    def add_holiday(self):
        date_str = self.date_entry.get()
        desc = self.desc_entry.get().strip()
        validated_date = validate_date(date_str)

        if not validated_date or not desc:
            messagebox.showerror("Erreur", "Veuillez entrer une date valide et une description.", parent=self)
            return

        date_sql = validated_date.strftime("%Y-%m-%d")
        if self.db.add_holiday(date_sql, desc, "Personnalisé"):
            self.desc_entry.delete(0, tk.END)
            self.date_entry.delete(0, tk.END)
            self.refresh_holidays_list()
        else:
            messagebox.showerror("Erreur", "Cette date est déjà enregistrée. Modifiez-la si besoin.", parent=self)

    def _on_holiday_select(self, event=None):
        is_selection = bool(self.holidays_tree.selection())
        new_state = "normal" if is_selection else "disabled"
        self.modify_button.config(state=new_state)
        self.delete_button.config(state=new_state)

    def modify_selected_holiday(self):
        selection = self.holidays_tree.selection()
        if not selection: return

        item = self.holidays_tree.item(selection[0])
        old_date_str, old_desc, old_type = item['values']
        old_date_sql = validate_date(old_date_str).strftime("%Y-%m-%d")

        # ... (La logique de la fenêtre de modification reste la même que dans la version 2.1.1)

    def delete_selected_holiday(self):
        selection = self.holidays_tree.selection()
        if not selection: return
        
        item = self.holidays_tree.item(selection[0])
        date_display, desc, _ = item['values']
        date_sql = validate_date(date_display).strftime("%Y-%m-%d")
        
        if messagebox.askyesno("Confirmation", f"Êtes-vous sûr de vouloir supprimer :\n{desc} ({date_display}) ?", parent=self):
            if self.db.delete_holiday(date_sql):
                self.refresh_holidays_list()
            else:
                messagebox.showerror("Erreur BD", "La suppression a échoué.", parent=self)

    def restore_auto_holidays(self):
        # ... (La logique de cette fonction reste la même que dans la version 2.1.1)
        pass

class JustificatifsWindow(tk.Toplevel):
    """
    Fenêtre Toplevel affichant la liste des congés maladie 
    pour lesquels un certificat médical est manquant.
    """
    def __init__(self, parent, db_manager):
        super().__init__(parent)
        self.db = db_manager
        
        self.title("Suivi des Justificatifs Médicaux Manquants")
        self.grab_set()
        self.geometry("800x500")

        self._create_widgets()
        self.refresh_list()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill="both", expand=True)
        
        cols = ("Agent", "PPR", "Date Début", "Date Fin", "Jours Pris")
        self.tree = ttk.Treeview(main_frame, columns=cols, show="headings", height=10)
        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120)
        self.tree.pack(fill="both", expand=True, padx=5, pady=5)
        
        ttk.Button(main_frame, text="Actualiser", command=self.refresh_list).pack(pady=5)

    def refresh_list(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        try:
            missing_certs = self.db.get_maladies_sans_certificat()
            for row in missing_certs:
                agent_nom = f"{row[0]} {row[1]}"
                ppr = row[2]
                debut = format_date_for_display(row[3])
                fin = format_date_for_display(row[4])
                jours = row[5]
                self.tree.insert("", "end", values=(agent_nom, ppr, debut, fin, jours))
        except sqlite3.Error as e:
            messagebox.showerror("Erreur BD", f"Impossible de charger la liste : {e}", parent=self)