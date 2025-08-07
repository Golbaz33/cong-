# ui/main_window.py

import tkinter as tk
from tkinter import ttk, messagebox
from collections import defaultdict, Counter
from dateutil import parser
import logging
import os
import sqlite3

# Import des composants de votre architecture
from core.conges.manager import CongeManager
from db.models import Agent, Conge
from ui.forms.agent_form import AgentForm
from ui.forms.conge_form import CongeForm
from ui.widgets.secondary_windows import HolidaysManagerWindow, JustificatifsWindow 
from ui.widgets.arabic_keyboard import ArabicKeyboard
from ui.widgets.date_picker import DatePickerWindow
from utils.file_utils import export_agents_to_excel, export_all_conges_to_excel, import_agents_from_excel
from utils.date_utils import format_date_for_display
from utils.config_loader import CONFIG

def treeview_sort_column(tv, col, reverse):
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    numeric_cols = ['Solde', 'Jours', 'PPR']
    try:
        if col in numeric_cols:
            l.sort(key=lambda t: float(str(t[0]).replace(',', '.')), reverse=reverse)
        else:
            l.sort(key=lambda t: str(t[0]).lower(), reverse=reverse)
    except (ValueError, IndexError):
        l.sort(key=lambda t: str(t[0]), reverse=reverse)
    
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    
    tv.heading(col, command=lambda: treeview_sort_column(tv, col, not reverse))


class MainWindow(tk.Tk):
    def __init__(self, manager: CongeManager):
        super().__init__()
        self.manager = manager
        self.db = self.manager.db

        self.title(f"{CONFIG['app']['title']} - v{CONFIG['app']['version']}")
        self.minsize(1200, 700)
            
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.current_page = 1
        self.items_per_page = 50
        self.total_pages = 1
        
        self.create_widgets()
        self.refresh_all()

    def on_close(self):
        if messagebox.askokcancel("Quitter", "Voulez-vous vraiment quitter ?"):
            self.db.close()
            self.destroy()

    def set_status(self, message):
        self.status_var.set(message)
        self.update_idletasks()

    def create_widgets(self):
        style = ttk.Style(self); style.theme_use('clam')
        style.configure("Treeview", rowheight=25, font=('Helvetica', 10))
        style.configure("Treeview.Heading", font=('Helvetica', 10, 'bold'))
        style.configure("TLabel", font=('Helvetica', 11))
        style.configure("TButton", font=('Helvetica', 10))
        style.configure("TLabelframe.Label", font=('Helvetica', 12, 'bold'))

        main_pane = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        main_pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        left_pane = ttk.Frame(main_pane, padding=5); main_pane.add(left_pane, weight=2)
        agents_frame = ttk.LabelFrame(left_pane, text="Agents"); agents_frame.pack(fill=tk.BOTH, expand=True)
        search_frame = ttk.Frame(agents_frame); search_frame.pack(fill=tk.X, padx=5, pady=5); ttk.Label(search_frame, text="Rechercher:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_var = tk.StringVar(); self.search_var.trace_add("write", lambda *args: self.search_agents())
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var); search_entry.pack(fill=tk.X, expand=True, side=tk.LEFT)
        
        cols_agents = ("ID", "Nom", "Prénom", "PPR", "Grade", "Solde");
        self.list_agents = ttk.Treeview(agents_frame, columns=cols_agents, show="headings", selectmode="browse")
        for col in cols_agents: self.list_agents.heading(col, text=col, command=lambda c=col: treeview_sort_column(self.list_agents, c, False))
        self.list_agents.column("ID", width=0, stretch=False); self.list_agents.column("Nom", width=120); self.list_agents.column("Prénom", width=120); self.list_agents.column("PPR", width=80, anchor="center"); self.list_agents.column("Grade", width=100); self.list_agents.column("Solde", width=60, anchor="center")
        self.list_agents.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.list_agents.bind("<<TreeviewSelect>>", self.on_agent_select)
        self.list_agents.bind("<Double-1>", lambda e: self.modify_selected_agent())
        
        pagination_frame = ttk.Frame(agents_frame); pagination_frame.pack(fill=tk.X, padx=5, pady=5)
        self.prev_button = ttk.Button(pagination_frame, text="<< Précédent", command=self.prev_page); self.prev_button.pack(side=tk.LEFT)
        self.page_label = ttk.Label(pagination_frame, text="Page 1 / 1"); self.page_label.pack(side=tk.LEFT, expand=True)
        self.next_button = ttk.Button(pagination_frame, text="Suivant >>", command=self.next_page); self.next_button.pack(side=tk.RIGHT)
        
        btn_frame_agents = ttk.Frame(agents_frame); btn_frame_agents.pack(fill=tk.X, padx=5, pady=(0, 5))
        ttk.Button(btn_frame_agents, text="Ajouter", command=self.add_agent_ui).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(btn_frame_agents, text="Modifier", command=self.modify_selected_agent).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(btn_frame_agents, text="Supprimer", command=self.delete_selected_agent).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        
        io_frame_agents = ttk.Frame(agents_frame); io_frame_agents.pack(fill=tk.X, padx=5, pady=(5, 5))
        ttk.Button(io_frame_agents, text="Importer Agents (Excel)", command=self.import_agents).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(io_frame_agents, text="Exporter Agents (Excel)", command=self.export_agents).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)

        right_pane = ttk.PanedWindow(main_pane, orient=tk.VERTICAL); main_pane.add(right_pane, weight=3)
        conges_frame = ttk.LabelFrame(right_pane, text="Congés de l'agent sélectionné"); right_pane.add(conges_frame, weight=3)
        filter_frame = ttk.Frame(conges_frame); filter_frame.pack(fill=tk.X, padx=5, pady=5); ttk.Label(filter_frame, text="Filtrer par type:").pack(side=tk.LEFT, padx=(0, 5))
        self.conge_filter_var = tk.StringVar(value="Tous"); conge_filter_combo = ttk.Combobox(filter_frame, textvariable=self.conge_filter_var, values=["Tous"] + CONFIG['ui']['types_conge'], state="readonly"); conge_filter_combo.pack(side=tk.LEFT, fill=tk.X, expand=True); conge_filter_combo.bind("<<ComboboxSelected>>", self.on_agent_select)
        
        cols_conges = ("CongeID", "Certificat", "Type", "Début", "Fin", "Jours", "Justification", "Intérimaire");
        self.list_conges = ttk.Treeview(conges_frame, columns=cols_conges, show="headings", selectmode="browse")
        for col in cols_conges: self.list_conges.heading(col, text=col, command=lambda c=col: treeview_sort_column(self.list_conges, c, False))
        self.list_conges.column("CongeID", width=0, stretch=False); self.list_conges.column("Certificat", width=80, anchor="center"); self.list_conges.column("Type", width=120); self.list_conges.column("Début", width=90, anchor="center"); self.list_conges.column("Fin", width=90, anchor="center"); self.list_conges.column("Jours", width=50, anchor="center"); self.list_conges.column("Intérimaire", width=150)
        self.list_conges.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.list_conges.tag_configure("summary", background="#e6f2ff", font=("Helvetica", 10, "bold"))
        self.list_conges.tag_configure("annule", foreground="grey", font=('Helvetica', 10, 'overstrike'))
        self.list_conges.bind("<Double-1>", lambda e: self.on_conge_double_click())
        
        btn_frame_conges = ttk.Frame(conges_frame); btn_frame_conges.pack(fill=tk.X, padx=5, pady=(0, 5));
        ttk.Button(btn_frame_conges, text="Ajouter", command=self.add_conge_ui).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(btn_frame_conges, text="Modifier", command=self.modify_selected_conge).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(btn_frame_conges, text="Supprimer", command=self.delete_selected_conge).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        
        stats_frame = ttk.LabelFrame(right_pane, text="Statistiques Globales"); right_pane.add(stats_frame, weight=1)
        self.text_stats = tk.Text(stats_frame, wrap=tk.WORD, font=('Courier New', 10), height=8, relief=tk.FLAT, background=self.cget('bg'))
        self.text_stats.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.text_stats.config(state=tk.DISABLED)
        
        global_actions_frame = ttk.Frame(stats_frame); global_actions_frame.pack(fill=tk.X, padx=5, pady=(0, 5))
        ttk.Button(global_actions_frame, text="Actualiser", command=self.refresh_stats).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(global_actions_frame, text="Suivi Justificatifs", command=self.open_justificatifs_suivi).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(global_actions_frame, text="Gérer les Jours Fériés", command=self.open_holidays_manager).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        ttk.Button(global_actions_frame, text="Exporter Tous les Congés", command=self.export_conges).pack(side=tk.LEFT, expand=True, fill=tk.X, padx=2)
        
        self.status_var = tk.StringVar(value="Prêt."); status_bar = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W); status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def get_selected_agent_id(self):
        selection = self.list_agents.selection()
        return int(self.list_agents.item(selection[0])["values"][0]) if selection else None

    def get_selected_conge_id(self):
        selection = self.list_conges.selection()
        if not selection: return None
        item = self.list_conges.item(selection[0])
        
        if "annule" in item["tags"] or "summary" in item["tags"]:
            return None
            
        return int(item["values"][0]) if item["values"] else None

    def add_agent_ui(self): AgentForm(self, self.manager)
    def modify_selected_agent(self):
        agent_id = self.get_selected_agent_id()
        if agent_id: AgentForm(self, self.manager, agent_id_to_modify=agent_id)
        else: messagebox.showwarning("Aucune sélection", "Veuillez sélectionner un agent à modifier.")
    def delete_selected_agent(self):
        agent_id = self.get_selected_agent_id()
        if not agent_id: messagebox.showwarning("Aucune sélection", "Veuillez sélectionner un agent à supprimer."); return
        agent = self.manager.get_agent_by_id(agent_id)
        if agent and self.manager.delete_agent_with_confirmation(agent.id, f"{agent.nom} {agent.prenom}"):
            self.set_status(f"Agent '{agent.nom} {agent.prenom}' supprimé."); self.refresh_all()
    def add_conge_ui(self):
        agent_id = self.get_selected_agent_id()
        if agent_id: CongeForm(self, self.manager, agent_id)
        else: messagebox.showwarning("Aucun agent", "Veuillez sélectionner un agent.")
    def modify_selected_conge(self):
        agent_id = self.get_selected_agent_id(); conge_id = self.get_selected_conge_id()
        if agent_id and conge_id: CongeForm(self, self.manager, agent_id, conge_id=conge_id)
        else: messagebox.showwarning("Aucune sélection", "Veuillez sélectionner un congé à modifier.")
    def delete_selected_conge(self):
        conge_id = self.get_selected_conge_id(); agent_id = self.get_selected_agent_id()
        if conge_id and self.manager.delete_conge_with_confirmation(conge_id):
            self.set_status("Congé supprimé."); self.refresh_all(agent_id)
        elif not conge_id: messagebox.showwarning("Aucune sélection", "Veuillez sélectionner un congé à supprimer.")
    def export_agents(self): export_agents_to_excel(self, self.db)
    def export_conges(self): export_all_conges_to_excel(self, self.db)
    def import_agents(self): import_agents_from_excel(self, self.db, on_success=self.refresh_all)
    def open_holidays_manager(self): HolidaysManagerWindow(self, self.db)
    def open_justificatifs_suivi(self): JustificatifsWindow(self, self.db)

    def refresh_all(self, agent_to_select_id=None):
        current_selection = agent_to_select_id or self.get_selected_agent_id()
        self.refresh_agents_list(current_selection)
        self.refresh_stats()

    def refresh_agents_list(self, agent_to_select_id=None):
        for row in self.list_agents.get_children(): self.list_agents.delete(row)
        term = self.search_var.get().strip().lower() or None
        total_items = self.manager.db.get_agents_count(term)
        self.total_pages = max(1, (total_items + self.items_per_page - 1) // self.items_per_page)
        self.current_page = min(self.current_page, self.total_pages)
        offset = (self.current_page - 1) * self.items_per_page
        agents = self.manager.get_all_agents(term=term, limit=self.items_per_page, offset=offset)

        selected_item_id = None
        for agent in agents:
            item_id = self.list_agents.insert("", "end", values=(agent.id, agent.nom, agent.prenom, agent.ppr, agent.grade, f"{agent.solde:.1f}"))
            if agent.id == agent_to_select_id:
                selected_item_id = item_id

        if selected_item_id:
            self.list_agents.selection_set(selected_item_id)
            self.list_agents.focus(selected_item_id)
        self.on_agent_select()
        
        self.page_label.config(text=f"Page {self.current_page} / {self.total_pages}")
        self.prev_button.config(state="normal" if self.current_page > 1 else "disabled")
        self.next_button.config(state="normal" if self.current_page < self.total_pages else "disabled")
        self.set_status(f"{len(agents)} agents affichés sur {total_items} au total.")

    def refresh_conges_list(self, agent_id):
        self.list_conges.delete(*self.list_conges.get_children())
        filtre = self.conge_filter_var.get()
        conges_data = self.manager.get_conges_for_agent(agent_id)
        
        conges_par_annee = defaultdict(list)
        for c in conges_data:
            if filtre != "Tous" and c.type_conge != filtre: continue
            try:
                conges_par_annee[c.date_debut.year].append(c)
            except AttributeError:
                logging.warning(f"Date invalide ou nulle pour congé ID {c.id}")
        
        for annee in sorted(conges_par_annee.keys(), reverse=True):
            total_jours = sum(c.jours_pris for c in conges_par_annee[annee] if c.type_conge == 'Congé annuel' and c.statut == 'Actif')
            summary_id = self.list_conges.insert("", "end", values=("", "", f"📅 ANNÉE {annee}", "", "", total_jours, f"{total_jours} jours pris"), tags=("summary",), open=True)
            
            for conge in conges_par_annee[annee]:
                cert_status = ""
                if conge.type_conge == 'Congé de maladie':
                    cert = self.db.get_certificat_for_conge(conge.id)
                    cert_status = "✅ Justifié" if cert else "❌ Manquant"
                
                interim_info = ""
                if conge.interim_id:
                    interim = self.manager.get_agent_by_id(conge.interim_id)
                    interim_info = f"{interim.nom} {interim.prenom}" if interim else "Agent Supprimé"
                
                tags_a_appliquer = ('annule',) if conge.statut == 'Annulé' else ()
                
                self.list_conges.insert(summary_id, "end", values=(
                    conge.id, cert_status, conge.type_conge, 
                    format_date_for_display(conge.date_debut), 
                    format_date_for_display(conge.date_fin), 
                    conge.jours_pris, conge.justif or "", interim_info
                ), tags=tags_a_appliquer)

    def refresh_stats(self):
        self.text_stats.config(state=tk.NORMAL)
        self.text_stats.delete("1.0", tk.END)
        try:
            all_conges = self.manager.db.get_conges()
            nb_agents = self.manager.db.get_agents_count()

            active_conges = [c for c in all_conges if c.statut == 'Actif']
            total_jours_pris = sum(c.jours_pris for c in active_conges)
            
            self.text_stats.insert(tk.END, f"{'Nombre total d\'agents':<25}: {nb_agents}\n")
            self.text_stats.insert(tk.END, f"{'Total des jours de congés actifs':<25}: {total_jours_pris}\n\n")
            self.text_stats.insert(tk.END, "Répartition par type de congé (actifs):\n")
            
            if active_conges:
                for type_conge, count in Counter(c.type_conge for c in active_conges).most_common():
                    self.text_stats.insert(tk.END, f"  - {type_conge:<22}: {count} ({(count / len(active_conges)) * 100:.1f}%)\n")
        except sqlite3.Error as e:
            self.text_stats.insert(tk.END, f"Erreur de lecture des statistiques: {e}")
        finally:
            self.text_stats.config(state=tk.DISABLED)

    def on_conge_double_click(self):
        conge_id = self.get_selected_conge_id()
        if not conge_id: return
        
        item = self.list_conges.item(self.list_conges.selection()[0])
        conge_type = item["values"][2]

        if conge_type == "Congé de maladie":
            cert = self.db.get_certificat_for_conge(conge_id)
            if cert and cert[4] and os.path.exists(cert[4]):
                try: os.startfile(os.path.realpath(cert[4]))
                except Exception as e: messagebox.showerror("Erreur d'ouverture", f"Impossible d'ouvrir le fichier:\n{e}", parent=self)
            else:
                messagebox.showinfo("Justificatif", "Aucun justificatif n'est attaché à ce congé.", parent=self)
        else:
             self.modify_selected_conge()

    def search_agents(self):
        self.current_page = 1; self.refresh_agents_list()
    def on_agent_select(self, event=None):
        agent_id = self.get_selected_agent_id()
        if agent_id:
            self.refresh_conges_list(agent_id)
        else:
            self.list_conges.delete(*self.list_conges.get_children())
    def prev_page(self):
        if self.current_page > 1: self.current_page -= 1; self.refresh_agents_list(self.get_selected_agent_id())
    def next_page(self):
        if self.current_page < self.total_pages: self.current_page += 1; self.refresh_agents_list(self.get_selected_agent_id())