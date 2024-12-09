import sqlite3
import os
import shutil


def backup_db(db_path):
    """Crée une sauvegarde de la base de données."""
    backup_path = f"{db_path}.backup"
    shutil.copy(db_path, backup_path)
    print(f"Save created at : {backup_path}")


def check_db(db_path):
    """Vérifie l'intégrité de la base de données SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute('PRAGMA integrity_check')
        result = cursor.fetchone()[0]
        if result == 'ok':
            print("SQLite integrity is ok!")
        else:
            print(f"Problem found: {result}.")
    except sqlite3.Error as e:
        print(f"{e}")
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
            print("Fast repair success")
        else:
            print("Fast repair failed")

        cursor.execute('VACUUM')
        print("SQLite optimized successfully")

    except sqlite3.Error as e:
        print(f"{e}")

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
            print("Found tables in SQLite :")
            for table in tables:
                print(f"- {table[0]}")
        else:
            print("No data found, corrupt?")
    except sqlite3.Error as e:
        print(f"{e}")
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
            print(f"{table_name} :")
            for row in rows:
                print(row)
        else:
            print(f"No data found, corrupt? {table_name}.")
    except sqlite3.Error as e:
        print(f"{table_name}: {e}")
    finally:
        conn.close()


def set_value(db_path, table_name, old_value, new_value):
    """Met à jour les valeurs d'une table spécifique dans la base de données SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(f"UPDATE {table_name} SET value = ? WHERE value = ?", (new_value, old_value))
        conn.commit()
        print(f"Value updated successfully {table_name}: {old_value} -> {new_value}")
    except sqlite3.Error as e:
        print(f"{table_name}: {e}")
    finally:
        conn.close()


def parse_db(db_path, target_db):
    """Convertit le dump SQLite pour une base de données cible (MySQL, MariaDB, etc.)."""
    with open(db_path, 'r') as file:
        sql_script = file.read()

    if target_db.lower() in ['mysql', 'mariadb']:
        # Remplacements généraux
        sql_script = sql_script.replace('AUTOINCREMENT', 'AUTO_INCREMENT')
        sql_script = sql_script.replace('INTEGER', 'INT')
        sql_script = sql_script.replace('TEXT', 'VARCHAR(255)')
        sql_script = sql_script.replace('BLOB', 'LONGBLOB')
        sql_script = sql_script.replace('REAL', 'DOUBLE')
        sql_script = sql_script.replace('NUMERIC', 'DECIMAL')
        sql_script = sql_script.replace('DATETIME', 'DATETIME')

        # Gestion des clés étrangères et des transactions
        sql_script = sql_script.replace('PRAGMA foreign_keys=OFF;', '')
        sql_script = sql_script.replace('BEGIN TRANSACTION;', 'START TRANSACTION;')
        sql_script = sql_script.replace('COMMIT;', 'COMMIT;')

        # Ajouter les remplacements pour les particularités SQL spécifiques
        sql_script = sql_script.replace('REFERENCES ', 'REFERENCES ')
        sql_script = sql_script.replace('DEFERRABLE INITIALLY DEFERRED', '')

        # Gestion des séquences
        sql_script = sql_script.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ')

    target_file = f"{os.path.splitext(db_path)[0]}_to_{target_db}.sql"
    with open(target_file, 'w') as file:
        file.write(sql_script)

    print(f"SQLite converted dropped into : {target_file}")


def dump_db(db_path, output_file):
    """Effectue un dump de la base de données SQLite dans un fichier .sql."""
    conn = sqlite3.connect(db_path)
    with open(output_file, 'w') as file:
        for line in conn.iterdump():
            file.write(f'{line}\n')
    print(f"Dumped SQLite file dropped in : {output_file}")


def main():
    while True:
        command = input("\nSQLite Tools : ")

        if command == "help":
            print("\nCommandes disponibles :")
            print("check <db_path> - Verify the database integrity")
            print("repair <db_path> - Repair the database")
            print("tables <db_path> - List all the available tables")
            print("tables value view <db_path> <TableName> - Display value of a specific table")
            #print(
            #    "tables value set <db_path> <TableName> <old_value> <new_value> - Met à jour une valeur spécifique dans la table")
            print("parse <db_path> <target_db> - Convert SQLite database into a another format")
            print("dump <db_path> <output_file> - Make a dump of the SQLite database")
            print("exit - Exit the program")
        elif command.startswith("check "):
            _, db_path = command.split(maxsplit=1)
            check_db(db_path)
        elif command.startswith("repair "):
            _, db_path = command.split(maxsplit=1)
            repair_db(db_path)
        elif command.startswith("tables "):
            parts = command.split()
            if len(parts) == 2:
                _, db_path = parts
                list_tables(db_path)
            elif len(parts) == 5 and parts[1] == "value" and parts[2] == "view":
                _, _, _, db_path, table_name = parts
                view_table(db_path, table_name)
            elif len(parts) == 6 and parts[1] == "value" and parts[2] == "set":
                _, _, _, db_path, table_name, old_value, new_value = parts
                set_value(db_path, table_name, old_value, new_value)
            else:
                print("Command not found. Try 'help' for more informations.")
        elif command.startswith("parse "):
            parts = command.split()
            if len(parts) == 3:
                _, db_path, target_db = parts
                parse_db(db_path, target_db)
            else:
                print("Command not found. Try 'help' for more informations.")
        elif command.startswith("dump "):
            parts = command.split()
            if len(parts) == 3:
                _, db_path, output_file = parts
                dump_db(db_path, output_file)
            else:
                print("Command not found. Try 'help' for more informations.")
        elif command == "exit":
            break
        else:
            print("Command not found. Try 'help' for more informations.")


if __name__ == "__main__":
    main()
