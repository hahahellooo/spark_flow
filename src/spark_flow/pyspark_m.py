import pandas as pd
import os
import shutil

def re_partition(load_dt='20150101'):
    home_dir = os.path.expanduser('~')
    extract_path=f'{home_dir}/data/movie/movie_data/data/extract/load_dt={load_dt}'
    rep_base=f'{home_dir}/data/movie/repartition/'
    rep_path=f'{rep_base}/load_dt={load_dt}'
    
    df = pd.read_parquet(extract_path)
    df['load_dt'] = load_dt
    partitions = [
        'load_dt','multiMovieYn','repNationCd']

    rm_dir(rep_path, load_dt)
    df.to_parquet(rep_base, partition_cols = partitions)
    return rep_path, df

def rm_dir(rep_path, load_dt):
    ep = os.path.join(rep_path, f'load_dt={load_dt}')
    if os.path.exists(ep):
        shutil.rmtree(ep)




    
