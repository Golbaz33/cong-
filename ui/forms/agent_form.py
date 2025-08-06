# ui/forms/agent_form.py
import tkinter as tk
from tkinter import ttk, messagebox
from utils.config_loader import CONFIG
from ui.widgets.arabic_keyboard import ArabicKeyboard

class AgentForm(tk.Toplevel):
    """
    Fenêtre de formulaire pour ajouter ou modifier un agent.
    Elle communique avec le manager pour la logique de sauvegarde.
    """
    def __init__(self, parent, manager, agent_id_to_modify=None):
        super().__init__(parent)
        self.parent = parent
        self.manager = manager
        self.agent_id = agent_id_to_modify
        self.is_modification = agent_id_to_modify is not None

        title = "Modifier un Agent" if self.is_modification else "Ajouter un Agent"
        self.title(title)
        self.grab_set()
        self.resizable(False, False)

        self._create_widgets()

        if self.is_modification:
            self._populate_data()

    def _populate_data(self):
        agent = self.manager.get_agent_by_id(self.agent_id)
        if not agent:
            messagebox.showerror("Erreur", "Agent introuvable.", parent=self)
            self.destroy()
            return
        
        self.entry_nom.insert(0, agent.nom)
        self.entry_prenom.insert(0, agent.prenom)
        self.entry_ppr.insert(0, agent.ppr)
        self.combo_grade.set(agent.grade)
        self.entry_solde.insert(0, f"{agent.solde:.1f}")


    def _create_widgets(self):
        frame = ttk.Frame(self, padding=10)
        frame.pack(fill="both", expand=True)
        
        labels = ["Nom:", "Prénom:", "PPR:", "Grade:", "Solde initial:"]
        for i, label_text in enumerate(labels):
            ttk.Label(frame, text=label_text).grid(row=i, column=0, sticky="w", padx=5, pady=5)

        # Nom
        nom_frame = ttk.Frame(frame)
        nom_frame.grid(row=0, column=1, sticky="ew")
        self.entry_nom = ttk.Entry(nom_frame)
        self.entry_nom.pack(side="left", expand=True, fill="x")
        tk.Button(nom_frame, text="🇸🇦", font=('Arial', 12), command=lambda: ArabicKeyboard(self, self.entry_nom), bd=1, relief="raised", bg="#f0f0f0").pack(side="left", padx=(5,0))

        # Prénom
        prenom_frame = ttk.Frame(frame)
        prenom_frame.grid(row=1, column=1, sticky="ew")
        self.entry_prenom = ttk.Entry(prenom_frame)
        self.entry_prenom.pack(side="left", expand=True, fill="x")
        tk.Button(prenom_frame, text="🇸🇦", font=('Arial', 12), command=lambda: ArabicKeyboard(self, self.entry_prenom), bd=1, relief="raised", bg="#f0f0f0").pack(side="left", padx=(5,0))

        # PPR
        self.entry_ppr = ttk.Entry(frame)
        self.entry_ppr.grid(row=2, column=1, sticky="ew")

        # Grade
        grades = CONFIG['ui']['grades']
        self.combo_grade = ttk.Combobox(frame, values=grades, state="readonly")
        self.combo_grade.grid(row=3, column=1, sticky="ew")
        if grades: self.combo_grade.set(grades[0])

        # Solde
        self.entry_solde = ttk.Entry(frame)
        self.entry_solde.grid(row=4, column=1, sticky="ew")
        if not self.is_modification:
            self.entry_solde.insert(0, "22.0")

        # Boutons
        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=5, columnspan=2, pady=10)
        ttk.Button(btn_frame, text="Valider", command=self._on_validate).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="Annuler", command=self.destroy).pack(side=tk.RIGHT)

    def _on_validate(self):
        try:
            agent_data = {
                'nom': self.entry_nom.get().strip(),
                'prenom': self.entry_prenom.get().strip(),
                'ppr': self.entry_ppr.get().strip(),
                'grade': self.combo_grade.get(),
                'solde': float(self.entry_solde.get().replace(",", "."))
            }

            if not all([agent_data['nom'], agent_data['ppr'], agent_data['grade']]):
                raise ValueError("Le nom, le PPR et le grade sont obligatoires.")
            if agent_data['solde'] < 0:
                raise ValueError("Le solde ne peut pas être négatif.")

            if self.is_modification:
                agent_data['id'] = self.agent_id
                success = self.manager.save_agent(agent_data, is_modification=True)
            else:
                success = self.manager.save_agent(agent_data)
            
            if success:
                message = "Agent modifié avec succès." if self.is_modification else "Agent ajouté avec succès."
                self.parent.set_status(message)
                self.parent.refresh_all(self.agent_id) # Rafraîchir et sélectionner l'agent
                self.destroy()
            else:
                messagebox.showerror("Erreur", f"Le PPR '{agent_data['ppr']}' est déjà utilisé.", parent=self)

        except ValueError as e:
            messagebox.showerror("Erreur de saisie", str(e), parent=self)
        except Exception as e:
            messagebox.showerror("Erreur Inattendue", f"Une erreur est survenue: {e}", parent=self)