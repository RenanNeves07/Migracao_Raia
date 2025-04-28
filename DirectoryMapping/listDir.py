import os
import pandas as pd
import re

arquivos = os.listdir(os.getenv('DOWNLOAD_PATH'))
df = pd.DataFrame(arquivos)
df.to_excel('Diretorio.xlsx')