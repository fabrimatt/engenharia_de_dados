# -*- coding: utf-8 -*-
"""MACD

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1uvqjfzFS5Nssl5vAXp88uqF0ZVn69SDN
"""

# Commented out IPython magic to ensure Python compatibility.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# %matplotlib inline

from google.colab import  files
arquivo = files.upload()

df =pd.read_csv('df_all_bovespa.csv', delimiter=';') #colocar aqui o seu arquivo do bovespa

df_itau= df[df['sigla_acao']== 'ITUB4']

df_itau.dtypes

df_itau['data_pregao']=pd.to_datetime(df_itau['data_pregao'],format='%Y-%m-%d')

df_itau.dtypes

df_itau_2020=df_itau[df_itau['data_pregao']>='2020-03-01']

df_itau_2020

df_itau_2020=df_itau_2020.set_index(pd.DatetimeIndex(df_itau_2020['data_pregao'].values))

df_itau_2020

rapidaMME=df_itau_2020.preco_fechamento.ewm(span=12).mean()

lentaMME=df_itau_2020.preco_fechamento.ewm(span=26).mean()

MACD=rapidaMME - lentaMME

sinal=MACD.ewm(span=9).mean()

plt.figure(figsize=(15,5))
plt.plot(df_itau_2020.index, MACD , label = 'Itau', color='blue')
plt.plot(df_itau_2020.index, sinal , label = 'sinal', color='orange')
plt.xticks(rotation=90)
plt.legend(loc='upper right')
plt.show()

df_itau_2020['MACD'] = MACD
df_itau_2020['sinal'] = sinal
df_itau_2020

import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

minimo=min([min(df_itau_2020['sinal']),min(df_itau_2020['MACD'])])
maximo=max([max(df_itau_2020['sinal']),max(df_itau_2020['MACD'])])



fig = make_subplots(vertical_spacing = 0, rows=2, cols=1, row_heights=[4,3])

fig.add_trace(go.Candlestick(x=df_itau_2020.index, open= df_itau_2020['preco_abertura'], high=df_itau_2020['preco_maximo'],
                          low=df_itau_2020['preco_minimo'], close= df_itau_2020['preco_fechamento']))


fig.add_trace(go.Scatter(x=df_itau_2020.index, y = df_itau_2020['MACD'], name='MACD', line = dict(color='blue')), row=2, col=1)
fig.add_trace(go.Scatter(x=df_itau_2020.index, y = df_itau_2020['sinal'], name='Sinal', line = dict(color='yellow')), row=2, col=1)



fig.update_layout(xaxis_rangeslider_visible=False,
                  xaxis=dict(zerolinecolor='black', showticklabels=False),
                  xaxis2=dict(showticklabels=False))
fig['layout']['yaxis2'].update(range=[minimo,maximo])

fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=False)
fig.show()