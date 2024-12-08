import sqlite3
import os
import shutil


def backup_db(db_path):
    """Crée une sauvegarde de la base de données."""
    backup_path = f"{db_path}.backup"
    shutil.copy(db_path, backup_path)
    print(f"Sauvegarde créée à: {backup_path}")


def check_db(db_path):
    """Vérifie l'intégrité de la base de données SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute('PRAGMA integrity_check')
        result = cursor.fetchone()[0]
        if result == 'ok':
            print("La base de données est intègre.")
        else:
            print(f"Problèmes détectés: {result}.")
    except sqlite3.Error as e:
        print(f"Erreur lors de la vérification: {e}")
    finally:
        conn.close()


def repair_db(db_path):
    """Répare la base de données SQLite."""
    backup_db(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute('PRAGMA quick_check')
        result = cursor.fetchone()[0]
        if result == 'ok':
            print("Réparation rapide réussie.")
        else:
            print("La réparation rapide a échoué.")

        cursor.execute('VACUUM')
        print("Base de données optimisée avec succès.")

    except sqlite3.Error as e:
        print(f"Erreur lors de la réparation: {e}")

    finally:
        conn.close()


def list_tables(db_path):
    """Liste toutes les tables de la base de données SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if tables:
            print("Tables dans la base de données :")
            for table in tables:
                print(f"- {table[0]}")
        else:
            print("Aucune table trouvée dans la base de données.")
    except sqlite3.Error as e:
        print(f"Erreur lors de la liste des tables: {e}")
    finally:
        conn.close()


def view_table(db_path, table_name):
    """Affiche les valeurs d'une table spécifique dans la base de données SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        if rows:
            print(f"Valeurs dans la table {table_name} :")
            for row in rows:
                print(row)
        else:
            print(f"Aucune donnée trouvée dans la table {table_name}.")
    except sqlite3.Error as e:
        print(f"Erreur lors de la lecture des données de la table {table_name}: {e}")
    finally:
        conn.close()


def parse_db(db_path, target_db):
    """Convertit le dump SQLite pour une base de données cible (MySQL, MariaDB, etc.)."""
    with open(db_path, 'r') as file:
        sql_script = file.read()

    if target_db.lower() in ['mysql', 'mariadb']:
        sql_script = sql_script.replace('AUTOINCREMENT', 'AUTO_INCREMENT')
        sql_script = sql_script.replace('INTEGER', 'INT')
        sql_script = sql_script.replace('TEXT', 'VARCHAR(255)')
        sql_script = sql_script.replace('PRAGMA foreign_keys=OFF;', '')
        sql_script = sql_script.replace('BEGIN TRANSACTION;', 'START TRANSACTION;')
        sql_script = sql_script.replace('COMMIT;', 'COMMIT;')

        # Remplacement pour la syntaxe spécifique aux clés étrangères
        sql_script = sql_script.replace('REFERENCES ', 'REFERENCES ')

        # Ajouter des remplacements supplémentaires si nécessaire
        # Par exemple, gérer les types de données ou autres particularités SQL

    target_file = f"{os.path.splitext(db_path)[0]}_to_{target_db}.sql"
    with open(target_file, 'w') as file:
        file.write(sql_script)

    print(f"Base de données convertie et enregistrée dans : {target_file}")


def dump_db(db_path, output_file):
    """Effectue un dump de la base de données SQLite dans un fichier .sql."""
    conn = sqlite3.connect(db_path)
    with open(output_file, 'w') as file:
        for line in conn.iterdump():
            file.write(f'{line}\n')
    print(f"Dump de la base de données enregistré dans : {output_file}")


def main():
    while True:
        command = input("\nSQLTools : ")

        if command == "help":
            print("\nCurrent available option\n")
            print("check <db_path> - Vérifie l'intégrité de la base de données")
            print("repair <db_path> - Répare la base de données")
            print("tables <db_path> - Liste toutes les tables de la base de données")
            print("tables value view <db_path> <TableName> - Affiche les valeurs d'une table spécifique")
            print("parse <db_path> <target_db> - Convertit la base de données pour la base de données cible")
            print("dump <db_path> <output_file> - Effectue un dump de la base de données dans un fichier .sql")
            print("exit - Quitte le programme")
        elif command.startswith("check"):
            _, db_path = command.split(maxsplit=1)
            check_db(db_path)
        elif command.startswith("repair"):
            _, db_path = command.split(maxsplit=1)
            repair_db(db_path)
        elif command.startswith("tables"):
            parts = command.split()
            if len(parts) == 2:
                _, db_path = parts
                list_tables(db_path)
            elif len(parts) == 5 and parts[1] == "value" and parts[2] == "view":
                _, _, _, db_path, table_name = parts
                view_table(db_path, table_name)
            else:
                print("Commande invalide. Tapez 'help' pour voir les commandes disponibles.")
        elif command.startswith("parse"):
            parts = command.split()
            if len(parts) == 3:
                _, db_path, target_db = parts
                parse_db(db_path, target_db)
            else:
                print("Commande invalide. Tapez 'help' pour voir les commandes disponibles.")
        elif command.startswith("dump"):
            parts = command.split()
            if len(parts) == 3:
                _, db_path, output_file = parts
                dump_db(db_path, output_file)
            else:
                print("Commande invalide. Tapez 'help' pour voir les commandes disponibles.")
        elif command == "exit":
            break
        else:
            print("Commande invalide. Tapez 'help' pour voir les commandes disponibles.")


if __name__ == "__main__":
    main()
