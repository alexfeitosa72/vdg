import pandas as pd
from pathlib import Path
from typing import Dict, Tuple
from dataclasses import dataclass
import os

__all__ = [
    'Paths',
    'consolidate_logs',
    'split_by_gender',
    'aggregate_data',
    'find_ties',
    'remove_ties',
    'get_removed_ties'
]

@dataclass
class Paths:
    """Centraliza todos os caminhos de arquivo"""
    base_dir: Path = Path('logs_em_tratamento')  # Relativo à pasta logs/
    consolidated: Path = base_dir / 'logs_totais_tratados.csv'
    male: Path = base_dir / 'logs_masculino.csv'
    female: Path = base_dir / 'logs_feminino.csv'
    male_agg: Path = base_dir / 'logs_masculino_agregado.csv'
    female_agg: Path = base_dir / 'logs_feminino_agregado.csv'
    male_agg_no_ties: Path = base_dir / 'logs_masculino_agregado_sem_empates.csv'
    female_agg_no_ties: Path = base_dir / 'logs_feminino_agregado_sem_empates.csv'
    ties: Path = base_dir / 'empates_duplos.csv'
    removed_ties: Path = base_dir / 'frases_removidas_por_empate.csv'

def consolidate_logs(paths: Paths) -> pd.DataFrame:
    """Consolida todos os arquivos de log em um único DataFrame"""
    # Ajusta o caminho para ser relativo ao script
    script_dir = Path(__file__).parent
    full_path = script_dir / paths.base_dir
    
    bloco_files = list(full_path.glob('bloco_*_concatenado.csv'))
    if not bloco_files:
        raise FileNotFoundError(
            f"Nenhum arquivo 'bloco_*_concatenado.csv' encontrado em {full_path}. "
            "Verifique se os arquivos de entrada estão no diretório correto."
        )
    dfs = [pd.read_csv(file, sep='\t') for file in bloco_files]
    return pd.concat(dfs, ignore_index=True)

def split_by_gender(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separa o DataFrame por gênero"""
    return df[df['GeneroCod'] == 'm'], df[df['GeneroCod'] == 'f']

def get_majority_class(group):
    """Determina a classificação majoritária"""
    value_counts = group.value_counts()
    if len(value_counts) > 1 and value_counts.iloc[0] == value_counts.iloc[1]:
        return 'empate'
    return value_counts.index[0]

def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega os dados por frase"""
    def get_totals(group):
        counts = group.value_counts()
        return {
            'classificacao_majoritaria': get_majority_class(group),
            'total_positiva': counts.get('positiva', 0),
            'total_negativa': counts.get('negativa', 0),
            'total_neutra': counts.get('neutra', 0)
        }

    agg_df = (df.groupby('frase')
              .agg({
                  'Value': get_totals,
                  'duracao': 'mean',
                  'ParticipantMD5': 'count'
              }))
    
    # Expande os resultados da agregação
    value_columns = pd.DataFrame(agg_df['Value'].tolist(), index=agg_df.index)
    agg_df = agg_df.drop('Value', axis=1)
    agg_df = pd.concat([agg_df, value_columns], axis=1)
    
    # Renomeia colunas
    agg_df = agg_df.rename(columns={
        'duracao': 'duracao_media',
        'ParticipantMD5': 'total_classificacoes'
    })
    
    return agg_df.reset_index()

def find_ties(male_agg: pd.DataFrame, female_agg: pd.DataFrame) -> pd.DataFrame:
    """Encontra frases com empate em ambos os gêneros"""
    empates = pd.merge(
        male_agg[male_agg['classificacao_majoritaria'] == 'empate'][['frase']],
        female_agg[female_agg['classificacao_majoritaria'] == 'empate'][['frase']],
        on='frase',
        how='inner'
    )
    
    return empates.merge(
        male_agg, 
        on='frase', 
        suffixes=('', '_m')
    ).merge(
        female_agg, 
        on='frase', 
        suffixes=('_m', '_f')
    )

def remove_ties(df: pd.DataFrame) -> pd.DataFrame:
    """Remove as frases que tiveram empate na classificação majoritária"""
    return df[df['classificacao_majoritaria'] != 'empate'].copy()

def get_removed_ties(male_agg: pd.DataFrame, female_agg: pd.DataFrame) -> pd.DataFrame:
    """Obtém as frases que foram removidas por terem empate em pelo menos um dos gêneros"""
    # Identifica frases com empate em cada gênero
    male_ties = male_agg[male_agg['classificacao_majoritaria'] == 'empate']['frase']
    female_ties = female_agg[female_agg['classificacao_majoritaria'] == 'empate']['frase']
    
    # Une as frases com empate de ambos os gêneros
    all_ties = pd.concat([male_ties, female_ties]).unique()
    
    # Cria DataFrame com informações detalhadas das frases removidas
    removed_data = []
    for frase in all_ties:
        male_row = male_agg[male_agg['frase'] == frase].iloc[0] if len(male_agg[male_agg['frase'] == frase]) > 0 else None
        female_row = female_agg[female_agg['frase'] == frase].iloc[0] if len(female_agg[female_agg['frase'] == frase]) > 0 else None
        
        removed_data.append({
            'frase': frase,
            'empate_masculino': 'Sim' if male_row is not None and male_row['classificacao_majoritaria'] == 'empate' else 'Não',
            'empate_feminino': 'Sim' if female_row is not None and female_row['classificacao_majoritaria'] == 'empate' else 'Não',
            'total_positiva_m': male_row['total_positiva'] if male_row is not None else 0,
            'total_negativa_m': male_row['total_negativa'] if male_row is not None else 0,
            'total_neutra_m': male_row['total_neutra'] if male_row is not None else 0,
            'total_positiva_f': female_row['total_positiva'] if female_row is not None else 0,
            'total_negativa_f': female_row['total_negativa'] if female_row is not None else 0,
            'total_neutra_f': female_row['total_neutra'] if female_row is not None else 0
        })
    
    return pd.DataFrame(removed_data)

def main():
    """Função principal que executa todo o pipeline"""
    paths = Paths()
    script_dir = Path(__file__).parent
    
    # Cria o diretório base se não existir
    full_path = script_dir / paths.base_dir
    if not full_path.exists():
        print(f"Criando diretório {full_path}")
        full_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # Consolidação
        df_consolidated = consolidate_logs(paths)
        df_consolidated.to_csv(script_dir / paths.consolidated, sep='\t', index=False)
        print(f"Arquivo consolidado criado com {len(df_consolidated)} registros")
        
        # Separação por gênero
        df_male, df_female = split_by_gender(df_consolidated)
        df_male.to_csv(script_dir / paths.male, sep='\t', index=False)
        df_female.to_csv(script_dir / paths.female, sep='\t', index=False)
        print(f"Registros masculinos: {len(df_male)}")
        print(f"Registros femininos: {len(df_female)}")
        
        # Agregação
        male_agg = aggregate_data(df_male)
        female_agg = aggregate_data(df_female)
        male_agg.to_csv(script_dir / paths.male_agg, sep='\t', index=False)
        female_agg.to_csv(script_dir / paths.female_agg, sep='\t', index=False)
        
        # Remove empates e salva novos arquivos
        male_agg_no_ties = remove_ties(male_agg)
        female_agg_no_ties = remove_ties(female_agg)
        male_agg_no_ties.to_csv(script_dir / paths.male_agg_no_ties, sep='\t', index=False)
        female_agg_no_ties.to_csv(script_dir / paths.female_agg_no_ties, sep='\t', index=False)
        print(f"Frases sem empate - Masculino: {len(male_agg_no_ties)}")
        print(f"Frases sem empate - Feminino: {len(female_agg_no_ties)}")
        
        # Análise de empates
        empates_df = find_ties(male_agg, female_agg)
        empates_df.to_csv(script_dir / paths.ties, sep='\t', index=False)
        print(f"Total de frases com empate duplo: {len(empates_df)}")
        
        # Salva informações sobre frases removidas
        removed_ties_df = get_removed_ties(male_agg, female_agg)
        removed_ties_df.to_csv(script_dir / paths.removed_ties, sep='\t', index=False)
        print(f"Total de frases removidas por empate: {len(removed_ties_df)}")
        
    except FileNotFoundError as e:
        print(f"Erro: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()