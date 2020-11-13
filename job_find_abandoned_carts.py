import os
import glob
import pandas as pd

def process_page_views(filepath, abandoned_carts_df_wrk):
    # LER OS ARQUIVOS E FAZ TRANSFORMAÇÕES
    # open log file
    df = pd.read_json(filepath, lines=True) 

    # convert timestamp column to string
    df.timestamp = df.timestamp.astype('string')
    
   # create same_customer and next_page column
    df['same_customer'] = df.customer.eq(df.customer.shift(-1))
    df['next_page'] = df.page.shift(-1)
     
    abandoned_carts = df[((df.page == 'basket') & (df.same_customer == False) & (df.next_page != 'checkout'))]
    abandoned_carts = abandoned_carts[['timestamp','customer','page','product']].values.tolist()
    abandoned_carts_df_wrk = abandoned_carts_df_wrk.append(pd.DataFrame(abandoned_carts, columns=['timestamp','customer','page','product']),ignore_index=True)
    return abandoned_carts_df_wrk


def export_json(filepath, df):
    # export to json
    df.to_json(filepath, orient='records', lines=True)
    

def process_data(filepath, func):
    # PEGA OS ARQUIVOS NOS DIRETÓRIOS E MANDA PROCESSAR
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    abandoned_carts_df = pd.DataFrame(columns=['timestamp','customer','page','product']) 

    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        abandoned_carts_df = func(datafile, abandoned_carts_df)
        print('{}/{} files processed.'.format(i, num_files))
   
    return abandoned_carts_df

def main():

    #abandoned_carts_df = pd.DataFrame(columns = ['timestamp','customer','page','product']) 
    abandoned_carts_df = process_data(filepath = os.path.join(os.getcwd(),'input'), func = process_page_views)
    export_json(filepath = os.path.join(os.getcwd(),'output/abandoned-carts.json'), df = abandoned_carts_df)

if __name__ == "__main__":
    main()