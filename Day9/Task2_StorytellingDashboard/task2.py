import seaborn as sns
import matplotlib.pyplot as plt

df = sns.load_dataset("titanic")

fig, axes = plt.subplots(2,2, figsize=(15,10))

fig.suptitle(
    "Story: How Passenger Class Influenced Survival",
    fontsize=18,
    fontweight="bold"
)

# 1
sns.countplot(
    data=df,
    x="class",
    hue="survived",
    ax=axes[0,0]
)
axes[0,0].set_title("Survival by Class")

# 2
sns.boxplot(
    data=df,
    x="class",
    y="fare",
    ax=axes[0,1]
)
axes[0,1].set_title("Fare Distribution")

# 3
sns.histplot(
    data=df,
    x="age",
    hue="survived",
    multiple="stack",
    ax=axes[1,0]
)
axes[1,0].set_title("Age Distribution")

# 4
numeric = df.select_dtypes(include="number")

sns.heatmap(
    numeric.corr(),
    annot=True,
    cmap="coolwarm",
    ax=axes[1,1]
)
axes[1,1].set_title("Correlation Heatmap")

plt.tight_layout()

plt.savefig(
    "storytelling_dashboard.png",
    dpi=300,
    bbox_inches="tight"
)

plt.show()

print("Task 2 Completed")