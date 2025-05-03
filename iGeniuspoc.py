#C:\MyPrograms\Python3.11 python3 
import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.connector.pandas_tools import write_pandas
import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt

connection_parameters = {
  "account": "MFIOAYW-ONB72516",
  "user": "A23455",
  "password": "testing321",
  "warehouse": "GRAPH_RAG",  # optional
  "database": "GRAPH_RAG",  # optional
  "schema": "GRAPH_RAG",  # optional
}

new_session = Session.builder.configs(connection_parameters).create()

# Create a Snowpark pandas DataFrame from existing Snowflake table
df_nodes = pd.read_snowflake('nodes')
print(df_nodes)
df_edges = pd.read_snowflake('edges')
print(df_edges)
#for node in df_nodes:
    #print (node)
    
    
# build a directed graph of the people and places they visit
G = nx.Graph()

# add node attributes (color + size)
for row in df_nodes.itertuples(index=False):
    #print (row)
    node_id = row[0]
    node_type = row[2]
    G.add_node(node_id,type=node_type)
    
# add edges
for row in df_edges.itertuples(index=False):
    G.add_edge(row[0], row[1], label=row[3])

# visualize using gravis
#gv.d3(G).display(inline=True)

# Render the graph using NetworkX and Matplotlib
fig, ax = plt.subplots()
pos = nx.spring_layout(G)  # Choose a layout
nx.draw_networkx_nodes(G, pos, node_color='lightblue')
nx.draw_networkx_edges(G, pos, edge_color='gray')
nx.draw_networkx_labels(G, pos, font_size=10)

# Display the graph in Streamlit
st.title("iGenius Knowledge Graph Visualization")
st.pyplot(fig)
# Close the connection
new_session.close()