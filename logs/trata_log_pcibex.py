# -*- coding: utf-8 -*-


import pandas as pd
from collections import Counter
import os

def process_log_file(file_path):
    # === 1. Read CSV file ===
    df = pd.read_csv(file_path, skiprows=19, header=None)
    df.columns = [
        "ReceptionTime", "ParticipantMD5", "Controller", "ItemNumber", "InnerElementNumber",
        "Label", "Group", "PennElementType", "PennElementName", "Parameter",
        "Value", "EventTime", "Comments"
    ]
    df = df[~df["ReceptionTime"].astype(str).str.startswith("#")].copy()
    df = df[~df["Label"].isin(["TCLE", "instrucoes", "agradecimento"])]
    df.reset_index(drop=True, inplace=True)

    # === 2. Extract gender data ===
    genero_data = df[
        (df["Label"] == "genero") &
        (df["PennElementName"] == "selecionaGenero") &
        (df["Parameter"] == "Selected")
    ][["ParticipantMD5", "Value"]].rename(columns={"Value": "Genero"}).drop_duplicates()

    # === 3. Process sentence block ===
    selecoes_frases = df[
        (df["Label"] == "frases") &
        (df["Parameter"] == "Selection")
    ].copy()
    selecoes_frases["ItemNumber"] = selecoes_frases["ItemNumber"].astype(int) - 3
    selecoes_frases = selecoes_frases[["ParticipantMD5", "ItemNumber", "Value", "EventTime"]]
    selecoes_frases.columns = ["ParticipantMD5", "ItemNumber", "Classificacao", "Timestamp"]

    # Get start and end times
    start_trials = df[(df["Label"] == "frases") & (df["Parameter"] == "_Trial_") & (df["Value"] == "Start")]
    end_trials = df[(df["Label"] == "frases") & (df["Parameter"] == "_Trial_") & (df["Value"] == "End")]
    start_trials = start_trials[["ParticipantMD5", "ItemNumber", "EventTime"]].copy()
    end_trials = end_trials[["ParticipantMD5", "ItemNumber", "EventTime"]].copy()
    start_trials["ItemNumber"] = start_trials["ItemNumber"].astype(int) - 3
    end_trials["ItemNumber"] = end_trials["ItemNumber"].astype(int) - 3
    start_trials.rename(columns={"EventTime": "StartTime"}, inplace=True)
    end_trials.rename(columns={"EventTime": "EndTime"}, inplace=True)

    # Calculate time spent
    frases_final = selecoes_frases.merge(start_trials, on=["ParticipantMD5", "ItemNumber"], how="left")
    frases_final = frases_final.merge(end_trials, on=["ParticipantMD5", "ItemNumber"], how="left")
    frases_final["Tempo_Gasto"] = ((frases_final["EndTime"] - frases_final["StartTime"]) / 1000).round(3)
    frases_final = frases_final[["ParticipantMD5", "ItemNumber", "Classificacao", "Tempo_Gasto", "Timestamp"]]
    
    return frases_final, genero_data

def main():
    all_processed_data = []
    base_item_number = 0
    
    # Use relative path for logs folder
    log_folder = os.path.join("tratamento_de_logs\logs_brutos")
    base_filename = "results_prod"
    
    # Process base file and numbered files
    files_to_process = [f"{base_filename}.csv"] + [f"{base_filename} ({i}).csv" for i in range(1, 10)]
    
    for filename in files_to_process:
        file_path = os.path.join(log_folder, filename)
        if not os.path.exists(file_path):
            continue
            
        print(f"Processing file: {filename}")
        try:
            frases, generos = process_log_file(file_path)
            
            # Add offset to ItemNumber for subsequent files
            frases["ItemNumber"] = frases["ItemNumber"] + base_item_number
            
            # Process this file's data independently
            generos["GeneroCod"] = generos["Genero"].str.lower().map(lambda g: "m" if "masculino" in g else "f")
            
            # Get 4 participants of each gender for this file
            ids_masculinos = generos[generos["GeneroCod"] == "m"].head(4)["ParticipantMD5"].tolist()
            ids_femininos = generos[generos["GeneroCod"] == "f"].head(4)["ParticipantMD5"].tolist()
            ids_selecionados = ids_femininos + ids_masculinos
            
            frases_filtradas = frases[frases["ParticipantMD5"].isin(ids_selecionados)].copy()

            # Create pivot tables with generic identifiers
            pivot_class = frases_filtradas.pivot_table(
                index="ItemNumber", 
                columns="ParticipantMD5", 
                values="Classificacao", 
                aggfunc="first"
            )
            
            pivot_time = frases_filtradas.pivot_table(
                index="ItemNumber", 
                columns="ParticipantMD5", 
                values="Tempo_Gasto", 
                aggfunc="first"
            )

            # Rename columns to generic identifiers
            fem_cols = [col for col in pivot_class.columns if generos.loc[generos["ParticipantMD5"] == col, "GeneroCod"].iloc[0] == "f"]
            masc_cols = [col for col in pivot_class.columns if generos.loc[generos["ParticipantMD5"] == col, "GeneroCod"].iloc[0] == "m"]

            rename_dict_class = {}
            rename_dict_time = {}
            
            for i, col in enumerate(fem_cols[:4], 1):
                rename_dict_class[col] = f"f{i}_class"
                rename_dict_time[col] = f"f{i}_tempo"
            
            for i, col in enumerate(masc_cols[:4], 1):
                rename_dict_class[col] = f"m{i}_class"
                rename_dict_time[col] = f"m{i}_tempo"

            pivot_class = pivot_class.rename(columns=rename_dict_class)
            pivot_time = pivot_time.rename(columns=rename_dict_time)
            
            frases_limitadas = pd.concat([pivot_class, pivot_time], axis=1).reset_index()

            # Calculate majority classifications
            def majoritaria(row, prefixo):
                colunas = [f"{prefixo}{i}_class" for i in range(1, 5)]
                respostas = [row[col] for col in colunas if pd.notna(row[col])]
                if respostas:
                    return Counter(respostas).most_common(1)[0][0]
                return None

            def contar_iguais_majoritaria(row, prefixo, majoritaria):
                colunas = [f"{prefixo}{i}_class" for i in range(1, 5)]
                return sum(1 for col in colunas if pd.notna(row[col]) and row[col] == row[majoritaria])

            frases_limitadas["cla_maj_femi"] = frases_limitadas.apply(lambda row: majoritaria(row, "f"), axis=1)
            frases_limitadas["cla_maj_masc"] = frases_limitadas.apply(lambda row: majoritaria(row, "m"), axis=1)
            
            frases_limitadas["qtd_maj_femi"] = frases_limitadas.apply(
                lambda row: contar_iguais_majoritaria(row, "f", "cla_maj_femi"), axis=1
            )
            frases_limitadas["qtd_maj_masc"] = frases_limitadas.apply(
                lambda row: contar_iguais_majoritaria(row, "m", "cla_maj_masc"), axis=1
            )

            # Organize columns
            colunas_final = [
                "ItemNumber",
                # Majorities and quantities
                "cla_maj_femi", "qtd_maj_femi",
                "cla_maj_masc", "qtd_maj_masc",
                # All female classifications together
                "f1_class", "f2_class", "f3_class", "f4_class",
                # All male classifications together
                "m1_class", "m2_class", "m3_class", "m4_class",
                # All time columns
                "f1_tempo", "f2_tempo", "f3_tempo", "f4_tempo",
                "m1_tempo", "m2_tempo", "m3_tempo", "m4_tempo"
            ]

            frases_formatadas = frases_limitadas[colunas_final]
            all_processed_data.append(frases_formatadas)
            
            # Update base_item_number for next file
            base_item_number = frases["ItemNumber"].max()
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
    
    if all_processed_data:
        # Combine all results and save
        resultado_final = pd.concat(all_processed_data, ignore_index=True)
        resultado_final.to_csv("log_tratado_completo.csv", index=False)
        print(f"Processed {len(resultado_final)} total records")
    else:
        print("No files were processed successfully!")

if __name__ == "__main__":
    main()