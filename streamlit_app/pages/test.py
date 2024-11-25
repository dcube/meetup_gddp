import pandas as pd
import plotly.express as px

# Sample data
data = {
    'DAG_NAME': ['DAG1', 'DAG1', 'DAG1', 'DAG2', 'DAG2'],
    'DATA_FORMAT_WAREHOUSE_SIZE': ['FORMAT1_SIZE1', 'FORMAT1_SIZE2', 'FORMAT2_SIZE1', 'FORMAT1_SIZE1', 'FORMAT2_SIZE2'],
    'DURATION': [10, 20, 30, 40, 50],
    'COST': [100, 200, 300, 400, 500]
}

# Create DataFrame
df = pd.DataFrame(data)

# Split DATA_FORMAT_WAREHOUSE_SIZE into separate columns
df[['DATA_FORMAT', 'WAREHOUSE_SIZE']] = df['DATA_FORMAT_WAREHOUSE_SIZE'].str.split('_', expand=True)

# Aggregate data
agg_df = df.groupby(['DAG_NAME', 'DATA_FORMAT', 'WAREHOUSE_SIZE']).agg({'DURATION': 'sum', 'COST': 'sum'}).reset_index()

# Create a Plotly treemap
fig = px.treemap(agg_df, path=['DAG_NAME', 'DATA_FORMAT', 'WAREHOUSE_SIZE'], values='DURATION',
                 color='COST', hover_data={'DURATION': True, 'COST': True},
                 color_continuous_scale='RdBu', title='DAG Treeview')

fig.show()
