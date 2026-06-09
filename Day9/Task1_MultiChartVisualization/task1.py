import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

os.makedirs("outputs", exist_ok=True)

df = sns.load_dataset("titanic")

# Histogram
plt.figure(figsize=(8,5))
plt.hist(df["age"].dropna(), bins=20)
plt.title("Age Distribution")
plt.xlabel("Age (Years)")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("outputs/histogram.png")
plt.close()

# Bar Chart
plt.figure(figsize=(8,5))
df["class"].value_counts().plot(kind="bar")
plt.title("Passenger Count by Class")
plt.xlabel("Passenger Class")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig("outputs/bar_chart.png")
plt.close()

# Line Chart
age_mean = df.groupby("pclass")["age"].mean()

plt.figure(figsize=(8,5))
plt.plot(age_mean.index, age_mean.values, marker="o")
plt.title("Average Age by Passenger Class")
plt.xlabel("Class")
plt.ylabel("Average Age")
plt.tight_layout()
plt.savefig("outputs/line_chart.png")
plt.close()

# Scatter Plot
plt.figure(figsize=(8,5))
plt.scatter(df["age"], df["fare"])
plt.title("Age vs Fare")
plt.xlabel("Age")
plt.ylabel("Fare")
plt.tight_layout()
plt.savefig("outputs/scatter_plot.png")
plt.close()

# Box Plot (Seaborn)
plt.figure(figsize=(8,5))
sns.boxplot(x="class", y="fare", data=df)
plt.title("Fare Distribution by Class")
plt.tight_layout()
plt.savefig("outputs/box_plot.png")
plt.close()

# Heatmap (Seaborn)
numeric = df.select_dtypes(include="number")

plt.figure(figsize=(10,6))
sns.heatmap(numeric.corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.savefig("outputs/heatmap.png")
plt.close()

print("Task 1 Completed")