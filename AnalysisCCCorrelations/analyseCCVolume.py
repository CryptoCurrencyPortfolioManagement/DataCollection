import pandas as pd

import seaborn as sns
from matplotlib import pyplot as plt


year = [2016, 2017, 2018]
data = [360*(2021-2016), 360*(2021-2017), 360*(2021-2018)]

df = pd.DataFrame(data= {"Year": year, "Number of Days": data})

# plot the total cc data volume over time
sns.barplot(data= df, x="Year", y="Number of Days", color= "#0b559f")
plt.title("Data available")
for i, (year_i, data_i) in enumerate(zip(year,data)):
    plt.annotate(text='{:.2%}'.format(data_i/data[0]), xy=(i, data_i + 10), ha='center')
plt.tight_layout()
plt.savefig("./plots/CcMarketDataVolume.png".format(year))
plt.show()
plt.close()
