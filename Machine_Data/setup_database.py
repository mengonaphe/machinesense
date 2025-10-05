import psycopg2
import psycopg2.extras
import json
import os
import uuid

# List of all parameter columns identified from Data.json
PARAMETER_COLUMNS = [
    "A001", "A002", "A003", "A004", "A007", "A008", "A009", "A010", "A011", "A012", "A013", "A014", "A015", "A016", "A017", "A018", "A019", "A020", "A021", "A022", "A023", "A024", "A027", "A028", "A029", "A030", "A031", "A034", "A035", "A036", "A037", "A038", "A039", "A040", "A041", "A042", "A043", "A044", "A045", "A046", "A048", "A049", "A050", "A051", "A052", "A053", "A054", "A055", "A056", "A057", "A058", "A059", "A060", "A061", "A062", "A063", "A064", "A065", "A066", "A067", "A068", "A069", "A071", "A072", "A073", "A074", "A075", "A076", "A077", "A078", "A079", "A080", "A081", "A082", "A083", "A084", "A085", "A086", "A087", "A088", "A089", "A090", "A091", "A092", "A093", "A094", "A095", "A096", "A097", "A098", "A099", "A100", "A101", "A102", "A104", "A105", "A106", "A107", "A109", "A110", "A111", "A112", "A113", "A114", "A115", "A116", "A117", "A118", "A119", "A120", "A121", "A122", "A123", "A124", "A125", "A126", "A127", "A128", "A129", "A130", "A131", "A132", "A133", "A134", "A135", "A136", "A137", "A138", "A139", "A140", "A141", "A142", "A143", "A144", "A145", "A146", "A147", "A148", "A149", "A150", "A151", "A152", "A153", "A159", "A160", "A161", "A162", "A163", "A164", "A165", "A166", "A167", "A168", "A169", "A178", "A179", "A180", "A181", "A182", "A183", "A184", "A185", "A186", "A187", "A188", "A189", "A190", "A191", "A196", "A197", "A198", "A199", "A200", "A201", "A202", "A203", "A204", "A205", "A206", "A207", "A208", "A209", "A210", "A211", "A212", "A213", "A214", "A215", "A216", "A217", "A218", "A219", "A220", "A221", "A222", "A223", "A224", "A225", "A226", "A227", "A228", "A229", "A230", "A237", "A238", "A239", "A243", "A244", "A245", "A246", "A247", "A248", "A249", "A250", "A251", "A252", "A253", "A255", "A257", "A258", "A259", "A260", "A261", "A262", "A263", "A264", "A265", "A266", "A267", "A268", "A270", "A271", "A272", "A273", "A274", "A275", "A276", "A277", "A278", "A279", "A280", "A281", "A282", "A283", "A284", "A285", "A286", "A287", "A288", "A289", "A290", "A291", "A292", "A293", "A294", "A299", "A303", "A304", "A305", "A306", "A307", "A308", "A309", "A315", "A316", "A324", "A325", "A326", "A327",
    "C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008", "C009", "C010", "C011", "C012", "C013", "C014", "C015", "C016", "C017", "C018", "C019", "C020", "C021", "C022", "C023", "C024", "C025", "C026", "C027", "C028", "C029", "C030", "C031", "C032", "C033", "C034", "C035", "C036", "C037", "C038", "C039", "C040", "C041", "C042", "C043", "C044", "C045", "C046", "C047", "C048", "C049", "C050", "C051", "C052", "C053", "C054", "C055", "C056", "C057", "C058", "C059", "C060", "C061", "C062", "C063", "C064", "C065", "C066", "C067", "C068", "C069", "C070", "C071", "C072", "C073", "C074", "C075", "C076", "C077", "C078", "C079", "C080", "C081", "C082", "C083", "C084", "C085", "C086", "C087", "C088", "C089", "C090", "C091", "C092", "C093", "C094", "C095", "C097", "C098", "C099", "C100", "C101", "C102", "C103", "C104", "C105", "C106", "C107", "C108", "C109", "C110", "C111", "C112", "C113", "C114", "C115", "C116", "C117", "C118", "C119", "C121", "C122", "C123", "C124", "C125", "C126", "C127", "C128", "C129", "C130", "C131", "C132", "C133", "C134", "C135", "C136", "C137", "C138", "C139", "C140", "C141", "C142", "C143", "C144", "C145", "C146", "C147", "C148", "C149", "C150", "C151", "C152", "C153", "C154", "C155", "C157", "C161", "C166", "C168", "C169", "C170", "C171", "C172", "C173", "C174", "C175", "C176", "C177", "C178", "C179", "C180", "C181", "C182", "C183", "C184", "C185", "C186", "C187", "C188", "C189", "C190", "C191", "C192", "C193", "C194", "C195", "C196", "C197", "C198", "C199", "C200", "C201", "C202", "C203", "C204", "C205", "C206", "C207", "C208", "C209", "C210", "C211", "C212", "C213", "C214", "C215", "C216", "C217", "C218", "C219", "C220", "C221", "C224",
    "S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008", "S009", "S010", "S011", "S012", "S013", "S014", "S015", "S016", "S017", "S018", "S019", "S020", "S021", "S022", "S023", "S024", "S025", "S026", "S027", "S028", "S031", "S032", "S033", "S034", "S035", "S036", "S037", "S039", "S040", "S041", "S042", "S043", "S045", "S046", "S047", "S048", "S049", "S050", "S051", "S052", "S053", "S055", "S056", "S057", "S058", "S059", "S060", "S061", "S062", "S063", "S064", "S065", "S066", "S067", "S068", "S069", "S070", "S071", "S072", "S073", "S074", "S075", "S076", "S077", "S078", "S079", "S080", "S081", "S082", "S083", "S084", "S085", "S088", "S089", "S091", "S094", "S097", "S098", "S099", "S100", "S101", "S102", "S103", "S104", "S107", "S108", "S109", "S110", "S111", "S112", "S113", "S114", "S115", "S116", "S117", "S118", "S119", "S120", "S121", "S122", "S123", "S124", "S125", "S126", "S127", "S128", "S129", "S130", "S131", "S132", "S133", "S134", "S135", "S136", "S137", "S138", "S139", "S140", "S141", "S142", "S143", "S146", "S147", "S148", "S149", "S150", "S151", "S152", "S154", "S155", "S156", "S157", "S158", "S160", "S161", "S164", "S165", "S166", "S167", "S170", "S175", "S176", "S177", "S178", "S179", "S180", "S181", "S182", "S183", "S184", "S185", "S186", "S187", "S188", "S189", "S190", "S191", "S192", "S193", "S194", "S195", "S196", "S197", "S198", "S199", "S200", "S201", "S202", "S203", "S204", "S205", "S206", "S207", "S208", "S209", "S210", "S211", "S212", "S213", "S214", "S216", "S217", "S218", "S219", "S220", "S221", "S223", "S225", "S226", "S227", "S228", "S229", "S230", "S231", "S232", "S233", "S234", "S235", "S236", "S246", "S249",
    "T001", "T002", "T003", "T004", "T007", "T008", "T009", "T010", "T011", "T012", "T013", "T014", "T015", "T016", "T017", "T018", "T019", "T020", "T021", "T022", "T023", "T024", "T025", "T026", "T029", "T030", "T031", "T032", "T033", "T034", "T035", "T036", "T037", "T038", "T039", "T040", "T041", "T042", "T043", "T044", "T045", "T046", "T047", "T049", "T050", "T051", "T052", "T053", "T054", "T055", "T056", "T057", "T058", "T059", "T060", "T061", "T062", "T063", "T064", "T065", "T066", "T067", "T068", "T069", "T070", "T071", "T072", "T073", "T074", "T075", "T076", "T077", "T078", "T079", "T080", "T081", "T082", "T084", "T085", "T086", "T087", "T088", "T089", "T090", "T091", "T092", "T093", "T094", "T095", "T096", "T100", "T106", "T107", "T108", "T109", "T110", "T111", "T112", "T113", "T114", "T115", "T116", "T117", "T118", "T119", "T120", "T121", "T122", "T123", "T124", "T125", "T126", "T127", "T128", "T129", "T130", "T131", "T132", "T133", "T134", "T135", "T136", "T137", "T138", "T139", "T140", "T141", "T142", "T143", "T144", "T145", "T146", "T147", "T148", "T149", "T150", "T153", "T154", "T155", "T156", "T157", "T158", "T159", "T160", "T161", "T162", "T164", "T165", "T166", "T167", "T168", "T169", "T170", "T171", "T172", "T173", "T174", "T175", "T176", "T177", "T178", "T186", "T187", "T189", "T190", "T191", "T192", "T193", "T194", "T196", "T197", "T198", "T200", "T201", "T202", "T203", "T204", "T206", "T207", "T208", "T209", "T210", "T211", "T212", "T213", "T214", "T215", "T216", "T217", "T218", "T219", "T220", "T221", "T222", "T223", "T224", "T225", "T226", "T227", "T228", "T229", "T239", "T240", "T241", "T242", "T253", "T254", "T255", "T256", "T257", "T258", "T261", "T262", "T263", "T273", "T274", "T275", "T276", "T277", "T278", "T279", "T280", "T282", "T283", "T284", "T285", "T286", "T288", "T289", "T290", "T291", "T292", "T293", "T295", "T296", "T297", "T298", "T299", "T300", "T302", "T303", "T304", "T305", "T306", "T307", "T311", "T320", "T321", "T322", "T323", "T324", "T325", "T326", "T327", "T329"
]

FIXED_COLUMNS = [
    "id", "recorded_at", "command", "machine_serial_number", "machine_type", "invocation_id"
]

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres"
    )

def create_table(cur):
    """Creates the machine_parameter_data table if it doesn't exist."""
    param_cols_sql = ", ".join([f'\"{col}\" TEXT' for col in PARAMETER_COLUMNS])
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS machine_parameter_data (
        id TEXT PRIMARY KEY,
        recorded_at TIMESTAMP NOT NULL,
        command TEXT,
        machine_serial_number TEXT NOT NULL,
        machine_type TEXT,
        invocation_id TEXT,
        {param_cols_sql}
    );
    """
    print("Creating table machine_parameter_data...")
    cur.execute(create_table_query)
    print("Table created or already exists.")

def create_hygiene_table(cur):
    """Creates the hygiene_protocol_data table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS hygiene_protocol_data (
        id TEXT PRIMARY KEY,
        machine_serial_number TEXT NOT NULL,
        machine_type TEXT,
        hygiene_protocol_code TEXT,
        time_stamp TIMESTAMP,
        request_id TEXT
    );
    """
    print("Creating table hygiene_protocol_data...")
    cur.execute(create_table_query)
    print("Table created or already exists.")

def create_error_log_table(cur):
    """Creates the error_log table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS error_log (
        id TEXT PRIMARY KEY NOT NULL,
        machine_serial_number TEXT NOT NULL,
        machine_type TEXT,
        error_type TEXT,
        error_set_at TIMESTAMP,
        error_quit_at TIMESTAMP
    );
    """
    print("Creating table error_log...")
    cur.execute(create_table_query)
    print("Table created or already exists.")

def populate_table(cur, data):
    """Populates the machine_parameter_data table from the given data."""
    print(f"Preparing to insert {len(data)} records...")
    
    all_columns = FIXED_COLUMNS + PARAMETER_COLUMNS
    
    # Prepare the INSERT statement
    insert_sql = f"INSERT INTO machine_parameter_data ({', '.join(f'\"{col}\"' for col in all_columns)}) VALUES %s ON CONFLICT (id) DO NOTHING"
    
    # Prepare the data for insertion
    records_to_insert = []
    for record in data:
        try:
            params = record.get('request', {}).get('json_body', {}).get('content', {}).get('parameter', {})
            header = record.get('request', {}).get('json_body', {}).get('header', {})

            values = (
                record.get('_id', {}).get('$oid'),
                record.get('recv_dt', {}).get('$date'),
                record.get('request', {}).get('json_body', {}).get('command'),
                header.get('serial_number_machine'),
                header.get('machine_type'),
                record.get('invocation_id'),
            ) + tuple(params.get(p) for p in PARAMETER_COLUMNS)
            
            records_to_insert.append(values)
        except KeyError as e:
            print(f"Skipping record due to missing key: {e}")
            continue

    # Execute the batch insert
    if records_to_insert:
        print(f"Inserting {len(records_to_insert)} records into the database...")
        psycopg2.extras.execute_values(cur, insert_sql, records_to_insert)
        print("Insertion complete.")
    else:
        print("No new records to insert.")

def populate_hygiene_table(cur, data):
    """Populates the hygiene_protocol_data table from the given data."""
    print(f"Preparing to insert hygiene protocol data for {len(data)} records...")
    
    insert_sql = "INSERT INTO hygiene_protocol_data (id, machine_serial_number, machine_type, hygiene_protocol_code, time_stamp, request_id) VALUES %s ON CONFLICT (id) DO NOTHING"

    records_to_insert = []
    for record in data:
        try:
            request_id = record.get('_id', {}).get('$oid')
            header = record.get('request', {}).get('json_body', {}).get('header', {})
            machine_serial_number = header.get('serial_number_machine')
            machine_type = header.get('machine_type')
            content = record.get('request', {}).get('json_body', {}).get('content', {})
            if isinstance(content, dict):
                hygiene_protocol = content.get('hygiene_protocol', {})
            else:
                hygiene_protocol = {}

            if isinstance(hygiene_protocol, dict):
                for time_stamp, code in hygiene_protocol.items():
                    values = (
                        str(uuid.uuid4()),
                        machine_serial_number,
                        machine_type,
                        code,
                        time_stamp,
                        request_id
                    )
                    records_to_insert.append(values)

        except KeyError as e:
            print(f"Skipping record due to missing key: {e}")
            continue

    # Execute the batch insert
    if records_to_insert:
        print(f"Inserting {len(records_to_insert)} hygiene protocol records into the database...")
        psycopg2.extras.execute_values(cur, insert_sql, records_to_insert)
        print("Hygiene protocol data insertion complete.")
    else:
        print("No new hygiene protocol records to insert.")

def populate_error_log_table(cur, data):
    """Populates the error_log table from the given data."""
    print(f"Preparing to insert error log data for {len(data)} records...")
    
    insert_sql = "INSERT INTO error_log (id, machine_serial_number, machine_type, error_type, error_set_at, error_quit_at) VALUES %s ON CONFLICT (id) DO NOTHING"

    latest_requests = {}
    for record in data:
        machine_serial_number = record.get('request', {}).get('json_body', {}).get('header', {}).get('serial_number_machine')
        if machine_serial_number:
            recv_dt = record.get('recv_dt', {}).get('$date')
            if machine_serial_number not in latest_requests or recv_dt > latest_requests[machine_serial_number].get('recv_dt', {}).get('$date'):
                latest_requests[machine_serial_number] = record

    records_to_insert = []
    for machine_serial_number, record in latest_requests.items():
        try:
            header = record.get('request', {}).get('json_body', {}).get('header', {})
            machine_type = header.get('machine_type')
            content = record.get('request', {}).get('json_body', {}).get('content', {})
            if isinstance(content, dict):
                error_log = content.get('error_log', [])
            else:
                error_log = []

            for error_entry in error_log:
                error_type = error_entry.get('error')
                set_timestamps = error_entry.get('set', [])
                quit_timestamps = error_entry.get('quit', [])

                for i in range(len(set_timestamps)):
                    error_set_at = set_timestamps[i]
                    error_quit_at = quit_timestamps[i] if i < len(quit_timestamps) else None
                    values = (
                        str(uuid.uuid4()),
                        machine_serial_number,
                        machine_type,
                        error_type,
                        error_set_at,
                        error_quit_at
                    )
                    records_to_insert.append(values)

        except KeyError as e:
            print(f"Skipping record due to missing key: {e}")
            continue

    # Execute the batch insert
    if records_to_insert:
        print(f"Inserting {len(records_to_insert)} error log records into the database...")
        psycopg2.extras.execute_values(cur, insert_sql, records_to_insert)
        print("Error log data insertion complete.")
    else:
        print("No new error log records to insert.")

def main():
    """Main function to set up the database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create the tables
        create_table(cur)
        create_hygiene_table(cur)
        create_error_log_table(cur)
        
        # Load data from JSON file
        json_file_path = os.path.join(os.path.dirname(__file__), 'Data.json')
        print(f"Loading data from {json_file_path}...")
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Populate the tables
        populate_table(cur, data)
        populate_hygiene_table(cur, data)
        populate_error_log_table(cur, data)
        
        # Commit the changes
        conn.commit()
        cur.close()
        
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to or working with PostgreSQL", error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()