import pandas as pd
import os

def process_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
        
        column_names = []
        data_start_index = 0
        for i, line in enumerate(lines):
            if "# Columns below this comment are as follows:" in line:
                data_start_index = i + 1
            elif data_start_index and line.startswith("#"):
                parts = line.strip("# \n").split(". ", 1)
                if len(parts) == 2:
                    column_names.append(parts[1])
            elif data_start_index and not line.startswith("#"):
                break
        
        df = pd.read_csv(file_path, comment="#", header=None)
        num_columns = df.shape[1]
        num_detected_columns = len(column_names)
        
        if num_detected_columns > num_columns:
            column_names = column_names[:num_columns]
        elif num_detected_columns < num_columns:
            column_names += [f"Column_{i+num_detected_columns+1}" for i in range(num_columns - num_detected_columns)]
        
        df.columns = column_names
        
        # Ensure required columns exist
        required_columns = [1, 3, 5, 10, 11]
        if any(i >= len(column_names) for i in required_columns):
            print("Error: Required columns not found in the file.")
            return None
        
        selected_columns = [
            column_names[1],  # Participante
            column_names[3],  # Ordem (Order number of item)
            column_names[5],  # Tipo
            column_names[10], # Valor
            column_names[11]  # Tempo
        ]
        df_selected = df[selected_columns]
        df_selected.columns = ["Participante", "Ordem", "Tipo", "Valor", "Tempo"]
        
        # Filter only 'frases' records
        df_selected = df_selected[df_selected["Tipo"] == "frases"].reset_index(drop=True)
        
        # Summarize every 3 records into 1
        grouped_data = []
        for i in range(0, len(df_selected), 3):
            if i + 2 < len(df_selected):
                participante = df_selected.loc[i, "Participante"]
                ordem = df_selected.loc[i, "Ordem"] - 3
                classificacao = df_selected.loc[i + 1, "Valor"]
                tempo_gasto = round((df_selected.loc[i + 2, "Tempo"] - df_selected.loc[i, "Tempo"]) / 1000, 2)
                grouped_data.append([participante, ordem, classificacao, tempo_gasto])
        
        df_final = pd.DataFrame(grouped_data, columns=["Participante", "Ordem", "Classificação", "Tempo Gasto"])
        
        output_file_path = file_path.replace(".csv", "_limpo.csv")
        df_final.to_csv(output_file_path, index=False)
        print(f"Arquivo processado e salvo como: {output_file_path}")
        
        return output_file_path
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return None

if __name__ == "__main__":
    file_name = input("Digite o nome do arquivo CSV (que está na mesma pasta do script): ")
    file_path = os.path.join(os.getcwd(), file_name)
    
    if not os.path.exists(file_path):
        print("Erro: Arquivo não encontrado.")
    else:
        process_file(file_path)
