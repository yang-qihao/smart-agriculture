import pandas as pd

df = pd.read_json('dirty_data.json', lines=True)
print("原始数据长度:", len(df))

cols = ["tem", "hum", "soil_hum", "light_inten"]

# 如果数据有缺失（向前向后填补）
df[cols] = df[cols].interpolate(method='linear')
df[cols] = df[cols].ffill().bfill()

# 使用正态分布去过滤数据（只针对 tem 列计算边界）
col_name = "tem"
temp_mean = df[col_name].mean()
temp_std = df[col_name].std()
# print(temp_std)

# 定义数据正常范围（3σ 原则）
lower_bound = temp_mean - 3 * temp_std
upper_bound = temp_mean + 3 * temp_std

print(f"{col_name} 的正常范围: [{lower_bound:.2f}, {upper_bound:.2f}]")

df_clean = df[(df[col_name] > lower_bound) & (df[col_name] < upper_bound)]

df_clean.to_json('clean_data.json', lines=True, force_ascii=False,orient='records')
print("处理后的数据长度:", len(df_clean))