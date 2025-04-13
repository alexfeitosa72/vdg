import pandas as pd
import os

def processar_log_pcibex(file_path):
    # Obter o diretório do script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_path)  # Garante que o caminho é relativo ao script
    
    if not os.path.isfile(file_path):
        print("Erro: Arquivo não encontrado. Certifique-se de fornecer um nome válido.")
        return
    
    # Gerar nome do arquivo de saída
    file_name, file_ext = os.path.splitext(os.path.basename(file_path))
    output_file = os.path.join(script_dir, f"{file_name}_processado{file_ext}")
    
    # Carregar o arquivo
    df = pd.read_csv(file_path)
    
    # Verificar as colunas disponíveis
    print("Colunas encontradas no arquivo:", df.columns.tolist())
    
    # Garantir que as colunas necessárias existem
    required_columns = ["col_0", "col_5", "col_8", "col_10", "col_11"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Erro: As seguintes colunas não foram encontradas no arquivo: {missing_columns}")
        return

    # Separar eventos de início e fim do trial
    df_trials = df[df["col_10"].isin(["Start", "End"]) & df["col_5"].eq("frases")].copy()
    df_trials = df_trials[["col_0", "col_8", "col_10", "col_11"]].copy()
    df_trials.columns = ["Participant_ID", "Frase_Numero", "Evento", "Timestamp"]
    df_trials["Frase_Numero"] = pd.to_numeric(df_trials["Frase_Numero"], errors="coerce")
    df_trials.dropna(subset=["Frase_Numero", "Timestamp"], inplace=True)
    df_trials["Timestamp"] = df_trials["Timestamp"].astype(int)

    # Separar eventos de classificação (positivo, negativo, neutro)
    df_classificacoes = df[df["col_10"].isin(["positiva", "negativa", "neutra"]) & df["col_8"].eq("classificacao")].copy()
    df_classificacoes = df_classificacoes[["col_0", "col_10", "col_11"]].copy()
    df_classificacoes.columns = ["Participant_ID", "Classificacao", "Timestamp"]
    df_classificacoes.dropna(subset=["Timestamp"], inplace=True)
    df_classificacoes["Timestamp"] = df_classificacoes["Timestamp"].astype(int)

    df_trials.sort_values(by=["Participant_ID", "Timestamp"], inplace=True)
    df_classificacoes.sort_values(by=["Participant_ID", "Timestamp"], inplace=True)

    registros = []
    temp_data = {}

    for _, row in df_trials.iterrows():
        participant_id = row["Participant_ID"]
        frase_num = row["Frase_Numero"]
        evento = row["Evento"]
        timestamp = row["Timestamp"]
        
        if (participant_id, frase_num) not in temp_data:
            temp_data[(participant_id, frase_num)] = {"Start_Trial": None, "End_Trial": None, "Classificacao": None}
        
        if evento == "Start":
            temp_data[(participant_id, frase_num)]["Start_Trial"] = timestamp
        elif evento == "End":
            temp_data[(participant_id, frase_num)]["End_Trial"] = timestamp

    for _, row in df_classificacoes.iterrows():
        participant_id = row["Participant_ID"]
        classificacao = row["Classificacao"]
        timestamp = row["Timestamp"]

        for (p_id, frase_num), valores in temp_data.items():
            if p_id == participant_id and valores["Start_Trial"] is not None and valores["End_Trial"] is not None:
                if valores["Start_Trial"] <= timestamp <= valores["End_Trial"]:
                    temp_data[(p_id, frase_num)]["Classificacao"] = classificacao
                    break

    for (participant_id, frase_num), valores in temp_data.items():
        start_trial = valores["Start_Trial"]
        end_trial = valores["End_Trial"]
        classificacao = valores["Classificacao"]
        
        if start_trial is not None and end_trial is not None and classificacao is not None:
            tempo_gasto = round((end_trial - start_trial) / 1000, 2)
            registros.append([participant_id, frase_num, start_trial, classificacao, end_trial, tempo_gasto])

    df_final = pd.DataFrame(registros, columns=["Participant_ID", "Frase_Numero", "Start_Trial", "Classificacao", "End_Trial", "Tempo_Gasto"])
    df_final.sort_values(by=["Participant_ID", "Frase_Numero"], inplace=True)

    df_final.to_csv(output_file, index=False)
    print(f"Processamento concluído! Arquivo salvo em: {output_file}")

if __name__ == "__main__":
    file_name = input("Digite o nome do arquivo CSV a ser processado (deve estar na mesma pasta do script): ").strip()
    processar_log_pcibex(file_name)
