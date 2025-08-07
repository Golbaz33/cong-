# db/models.py
from utils.date_utils import validate_date

class Agent:
    """Représente un agent avec ses attributs."""
    def __init__(self, id, nom, prenom, ppr, grade, solde):
        self.id = id
        self.nom = nom
        self.prenom = prenom
        self.ppr = ppr
        self.grade = grade
        self.solde = float(solde)

    def __str__(self):
        return f"{self.nom} {self.prenom} (PPR: {self.ppr})"

    @classmethod
    def from_db_row(cls, row):
        """Crée une instance de Agent à partir d'une ligne de la base de données."""
        if not row:
            return None
        return cls(id=row[0], nom=row[1], prenom=row[2], ppr=row[3], grade=row[4], solde=row[5])

class Conge:
    """Représente un congé avec ses attributs."""
    def __init__(self, id, agent_id, type_conge, justif, interim_id, date_debut, date_fin, jours_pris, statut='Actif'):
        self.id = id
        self.agent_id = agent_id
        self.type_conge = type_conge
        self.justif = justif
        self.interim_id = interim_id
        self.date_debut = validate_date(date_debut) # Convertit la chaîne en objet datetime
        self.date_fin = validate_date(date_fin)     # Convertit la chaîne en objet datetime
        self.jours_pris = jours_pris
        self.statut = statut

    def __str__(self):
        debut_str = self.date_debut.strftime('%d/%m/%Y') if self.date_debut else 'N/A'
        fin_str = self.date_fin.strftime('%d/%m/%Y') if self.date_fin else 'N/A'
        return f"Congé {self.type_conge} du {debut_str} au {fin_str} ({self.jours_pris} jours)"

    @classmethod
    def from_db_row(cls, row):
        """Crée une instance de Conge à partir d'une ligne de la base de données."""
        if not row:
            return None
        # L'ordre des colonnes doit correspondre à la requête SELECT
        return cls(
            id=row[0], 
            agent_id=row[1], 
            type_conge=row[2], 
            justif=row[3], 
            interim_id=row[4], 
            date_debut=row[5], 
            date_fin=row[6], 
            jours_pris=row[7],
            statut=row[8]
        )