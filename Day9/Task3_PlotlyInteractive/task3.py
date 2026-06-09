import seaborn as sns
import plotly.express as px

df = sns.load_dataset("titanic")

# Scatter
fig = px.scatter(
    df,
    x="age",
    y="fare",
    color="class",
    hover_data=["sex","survived"]
)

fig.write_html("interactive_scatter.html")

# Histogram
fig = px.histogram(
    df,
    x="age",
    color="survived"
)

fig.write_html("interactive_histogram.html")

# Box Plot
fig = px.box(
    df,
    x="class",
    y="fare",
    color="class"
)

fig.write_html("interactive_boxplot.html")

# Treemap
fig = px.treemap(
    df,
    path=["class","sex"],
    values="fare"
)

fig.write_html("interactive_treemap.html")

print("Task 3 Completed")