import os
import psycopg2
import pandas as pd
import time
import logging
import psycopg2.extras as pg_extras

class Etl():
    def __init__(self):
        self.origin_db_connection = None
        self.destination_db_connection = None
        self.data = None
    def connect(self) -> int:
        try:
            self.origin_db_connection = psycopg2.connect(
                host=os.getenv('ORIGIN_DB_HOST'),
                port=os.getenv('ORIGIN_DB_PORT'),
                database=os.getenv('ORIGIN_DB_NAME'),  
                user=os.getenv('ORIGIN_DB_USER'),
                password=os.getenv('ORIGIN_DB_PASSWORD'))
            
            self.destination_db_connection = psycopg2.connect(
                host=os.getenv('DESTINATION_DB_HOST'),
                port=os.getenv('DESTINATION_DB_PORT'), 
                database=os.getenv('DESTINATION_DB_NAME'),
                user=os.getenv('DESTINATION_DB_USER'),
                password=os.getenv('DESTINATION_DB_PASSWORD')
            )
            return 0
        except Exception as e:
            print(e)
            return -1    
    def execute_query(self, connection, query)-> int:
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            result = cursor.fetchall() 
            cursor.close()
            return result if result else 0
        except Exception as e :
            print(e)
            return -1
    def extract(self)-> int:
        try:
            result = self.execute_query(self.origin_db_connection, "SELECT * FROM coviddata")  # This should return a list of tuples
            self.data = pd.DataFrame(result, columns=[column[0] for column in self.execute_query(self.origin_db_connection, "SELECT column_name FROM information_schema.columns WHERE table_name = 'coviddata'")])  # Get column names and create DataFrame
            return 0  # Return the DataFrame
        except psycopg2.Error as e:  # Catch specific psycopg2 errors (database-related)
            print(f"Error executing query: {e}")
            return -1
        except Exception as e:  # Catch general exceptions
            print(f"Unexpected error: {e}")
            return -1     
    def transform(self)-> int:
        try:
            start_time = time.time()  # Start timer

            # Configure logging (optional)
            logging.basicConfig(level=logging.INFO)

            df = self.data
            logging.info("Starting data transformation...")

            # Define transformation functions with error handling
            def fill_mean(df):
                numeric_cols = df.select_dtypes(include=['number']).columns
                for col in numeric_cols:
                    try:
                        df[col] = df[col].fillna(df[col].mean())
                    except Exception as e:
                        logging.error(f"Error filling mean for column {col}: {e}")
                return df

            def fill_columns(df):
                columns_to_fill = {
                    'new_cases': 0,
                    'total_cases': 'ffill',
                    'new_deaths': 0,
                    'total_deaths': 'ffill',
                    'population': 'ffill'
                }

                for col, method in columns_to_fill.items():
                    if col in df.columns:
                        try:
                            if method == 'ffill':
                                df[col] = df[col].ffill()
                            else:
                                df[col] = df[col].fillna(method)
                        except Exception as e:
                            logging.error(f"Error filling {method} for column {col}: {e}")
                return df

            # Apply transformations
            logging.info("Applying data transformation...")

            # Combine transformations into a single function to avoid multiple passes over the DataFrame
            def combined_transformations(df):
                df = fill_mean(df)
                df = fill_columns(df)
                return df

            # Apply the combined transformations
            df = combined_transformations(df)

            elapsed_time = time.time() - start_time  # Final elapsed time
            logging.info(f"Data transformation complete (elapsed time: {elapsed_time:.2f} seconds)")

            self.data = df
            return 0
        except Exception as e:
            logging.error(f"Error during data transformation: {e}")
            return -1   
    def preload(self) -> int:
        def generate_ddl_for_columns(columns):
            ddl_query = "CREATE TABLE IF NOT EXISTS public.output_storage (\n"
            
            for i, column in enumerate(columns):
                ddl_query += f"    {column} VARCHAR(200) NULL"
                if i < len(columns) - 1:
                    ddl_query += ",\n"
            
            ddl_query += "\n);"
            
            return ddl_query
        try:
            # Excluir a tabela output_storage, se ela existir
            drop_table_query = "DROP TABLE IF EXISTS public.output_storage;"
            self.execute_query(self.destination_db_connection, drop_table_query)
            
            # Gerar o DDL para criar a tabela com base nas colunas do DataFrame
            ddl_query = generate_ddl_for_columns(self.data.columns)
            print(ddl_query)
            # Executar a consulta DDL para criar a tabela
            self.execute_query(self.destination_db_connection, ddl_query)
            
            return 0
        except Exception as e:
            print(f"Error during preloading: {e}")
            return -1

    def load(self)-> int:
        batch_size = 1000  # Ajuste o tamanho do lote conforme necessário

        try:
            # Configure o logging (opcional)
            logging.basicConfig(level=logging.INFO)

            # Conexão com o banco de dados de destino (opcional)
            with self.destination_db_connection:
                # Consulta para obter os nomes das colunas da tabela destino
                columns_query = """SELECT column_name
                                FROM information_schema.columns
                                WHERE table_name = 'coviddata';"""
                columns_raw = self.execute_query(self.origin_db_connection, columns_query)
                column_list = [column[0] for column in columns_raw]
                column_names = ', '.join(column_list)

                # Construa a consulta SQL de inserção
                insert_query = f"INSERT INTO public.output_storage ({column_names}) VALUES %s"

                # Converta o DataFrame em uma lista de registros
                data_list = self.data.to_records(index=False)

                total_rows = len(data_list)
                processed_rows = 0
                batch_number = 1

                start_time = time.time()

                # Divida a lista de dados em lotes e insira-os no banco de dados
                for i in range(0, total_rows, batch_size):
                    batch = data_list[i:i+batch_size]
                    pg_extras.execute_values(self.destination_db_connection.cursor(), insert_query, batch, template=None)
                    self.destination_db_connection.commit()

                    processed_rows += len(batch)
                    progress = processed_rows / total_rows * 100

                    # Atualize o progresso a cada 10 lotes ou no final
                    if (batch_number % 10 == 0) or (batch_number == int(total_rows / batch_size) + 1):
                        logging.info(f"Batch {batch_number}/{int(total_rows / batch_size)}: {processed_rows} rows processed ({progress:.2f}%)")

                    batch_number += 1

                elapsed_time = time.time() - start_time
                logging.info(f"Data loading complete in {elapsed_time:.2f} seconds")

        except Exception as e:
            logging.error(f"Error during data loading: {e}")
            return -1

        return 0
    def run(self)->int:
        try:
            print("Connecting...",self.connect())
            print("Extracting...",self.extract())
            print("Transforming...",self.transform())
            print("Preload...",self.preload())
            print("Loading...", self.load())
            return 0
        except Exception as e:
            print(f"Error: {e}")
            return -1
    
if __name__ == "__main__":
    etl = Etl()
    etl.run()