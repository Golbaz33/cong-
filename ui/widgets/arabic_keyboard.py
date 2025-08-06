# ui/widgets/arabic_keyboard.py
import tkinter as tk

# Constantes pour le style du clavier, peuvent être déplacées dans config.yaml si besoin
ARABIC_LAYOUT = [ "ضصثقفغعهخحجد", "شسيبلاتنمكط", "ئءؤرلاىةوزظ" ]
KEYBOARD_BG = "#f0f0f0"
KEYBOARD_BTN_BG = "#ffffff"
KEYBOARD_BTN_ACTIVE = "#e0e0e0"
KEYBOARD_SPECIAL_BG = "#d4e6f1"
KEYBOARD_FONT = ('Arial', 14, 'bold')

class ArabicKeyboard(tk.Toplevel):
    """
    Crée une fenêtre TopLevel contenant un clavier arabe virtuel pour remplir un champ de saisie.
    """
    def __init__(self, parent, entry_widget):
        super().__init__(parent)
        self.entry_widget = entry_widget
        
        self.title("Clavier Arabe")
        self.resizable(False, False)
        self.transient(parent) # Reste au-dessus de la fenêtre parente
        self.grab_set()        # Modal : bloque les interactions avec la fenêtre parente

        self.configure(bg=KEYBOARD_BG)
        
        self._create_widgets()
        self._bind_events()
        self._update_preview()
        
        # Positionner la fenêtre sous le champ de saisie
        parent.update_idletasks()
        x = entry_widget.winfo_rootx()
        y = entry_widget.winfo_rooty() + entry_widget.winfo_height() + 5
        self.geometry(f"+{x}+{y}")

    def _create_widgets(self):
        main_frame = tk.Frame(self, bg=KEYBOARD_BG, padx=10, pady=10)
        main_frame.pack(fill="both", expand=True)

        # --- Cadre du haut : Aperçu et bouton Fermer ---
        top_frame = tk.Frame(main_frame, bg=KEYBOARD_BG)
        top_frame.pack(fill="x", pady=5)
        
        close_btn = tk.Button(
            top_frame, text="× Fermer", font=('Arial', 10), 
            command=self.destroy, bd=0, bg=KEYBOARD_SPECIAL_BG, 
            activebackground=KEYBOARD_BTN_ACTIVE
        )
        close_btn.pack(side="right", padx=5)

        self.preview_label = tk.Label(
            top_frame, text="", font=('Arial', 12), bg="white",
            relief="sunken", width=40, anchor="w", padx=5
        )
        self.preview_label.pack(side="left", fill="x", expand=True)

        # --- Cadre des touches ---
        keys_frame = tk.Frame(main_frame, bg=KEYBOARD_BG)
        keys_frame.pack(fill="both", expand=True)
        
        for row_chars in ARABIC_LAYOUT:
            row_frame = tk.Frame(keys_frame, bg=KEYBOARD_BG)
            row_frame.pack(fill="x", pady=2)
            for char in row_chars:
                btn = tk.Button(
                    row_frame, text=char, font=KEYBOARD_FONT,
                    width=3, bd=1, relief="raised", bg=KEYBOARD_BTN_BG,
                    activebackground=KEYBOARD_BTN_ACTIVE,
                    command=lambda c=char: self._on_key_press(c)
                )
                btn.pack(side="left", padx=2, pady=2, fill="both", expand=True)

        # --- Cadre du bas : Touches spéciales ---
        bottom_frame = tk.Frame(keys_frame, bg=KEYBOARD_BG)
        bottom_frame.pack(fill="x", pady=(10,0))
        
        space_btn = tk.Button(
            bottom_frame, text="Espace", font=('Arial', 10, 'bold'),
            bg=KEYBOARD_SPECIAL_BG, activebackground=KEYBOARD_BTN_ACTIVE,
            padx=10, command=lambda: self._on_key_press(" ")
        )
        space_btn.pack(side="left", expand=True, fill="x", padx=2)
        
        back_btn = tk.Button(
            bottom_frame, text="Effacer ⌫", font=('Arial', 10, 'bold'),
            bg=KEYBOARD_SPECIAL_BG, activebackground=KEYBOARD_BTN_ACTIVE,
            padx=10, command=self._backspace
        )
        back_btn.pack(side="left", expand=True, fill="x", padx=2)

        clear_btn = tk.Button(
            bottom_frame, text="Vider ✕", font=('Arial', 10, 'bold'),
            bg=KEYBOARD_SPECIAL_BG, activebackground=KEYBOARD_BTN_ACTIVE,
            padx=10, command=self._clear
        )
        clear_btn.pack(side="left", expand=True, fill="x", padx=2)

    def _bind_events(self):
        """Associe la touche Échap à la fermeture de la fenêtre."""
        self.bind("<Escape>", lambda e: self.destroy())

    def _on_key_press(self, char):
        """Insère un caractère dans le champ de saisie et met à jour l'aperçu."""
        self.entry_widget.insert(tk.END, char)
        self._update_preview()
        self.entry_widget.focus_set() # Redonne le focus au champ de saisie

    def _backspace(self):
        """Supprime le dernier caractère du champ de saisie."""
        current_text = self.entry_widget.get()
        if current_text:
            self.entry_widget.delete(len(current_text)-1, tk.END)
            self._update_preview()
        self.entry_widget.focus_set()

    def _clear(self):
        """Vide complètement le champ de saisie."""
        self.entry_widget.delete(0, tk.END)
        self._update_preview()
        self.entry_widget.focus_set()

    def _update_preview(self):
        """Met à jour le label d'aperçu avec le contenu actuel du champ de saisie."""
        current_text = self.entry_widget.get()
        # Affiche seulement la fin du texte s'il est trop long
        if len(current_text) > 35:
            display_text = "... " + current_text[-35:]
        else:
            display_text = current_text or "Aperçu..."
        self.preview_label.config(text=display_text)