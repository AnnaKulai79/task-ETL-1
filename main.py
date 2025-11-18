import pandas as pd
import numpy as np


URL = "https://s3-eu-west-1.amazonaws.com/shanebucket/downloads/uk-500.csv"

df = pd.read_csv(URL)

# print(df.head())
# df.info()
# print(df.describe())
# print(df.isna().sum()) #  пропущені
# print(df.duplicated().sum()) #  дублікати 
# print(df.columns) 
# ['first_name', 'last_name', 'company_name', 'address', 'city', 'county','postal', 'phone1', 'phone2', 'email', 'web']
# print(df.loc[5])

df['email'] = df['email'].str.lower()
df['web'] = df['web'].str.lower()
df['phone1'] = df['phone1'].str.replace(r'[^\d]+', '', regex=True)
df['phone2'] = df['phone2'].str.replace(r'[^\d]+', '', regex=True)
# print(df[['address', 'city']])
# a_dataframe.insert(loc=1, column="B", value=[4, 5, 6])
df.insert(loc=2, column="full_name", value=(df["first_name"] + " " + df["last_name"]))
# df["full_name"] = df["first_name"] + " " + df["last_name"]
# print(df.columns)
# print(df.loc[5])
df["email_domain"] = df["email"].str.split("@").str[-1]
df["city_length"] = df["city"].str.len()
df['is_gmail'] = df['email_domain'] == 'gmail.com'

# 4. Фільтрація даних Створити підвибірки:
# 4.1 користувачі з доменом gmail.com
df_gmail = df.loc[df['email_domain'] == 'gmail.com']
# print(df_gmail[['full_name']])
# 4.2 працівники компаній з “LLC” або “Ltd”
df_llc_ltd = df.loc[df['company_name'].str.contains('LLC|Ltd', case=False, na=False, regex=True)]
# print(df_llc_ltd[['full_name', 'company_name']])
# 4.3 люди з міста London
df_london = df.loc[df['city'] == 'London']
# print(df_london)
# # 4.4 компанії з назвою ≥ 4 слів
df_more_four = df.loc[df['company_name'].str.split().str.len() >= 4]
# print(df_more_four[['full_name', 'company_name']])

# 5. Позиційна вибірка (iloc)
# print(df.iloc[:10, 1:5])
# print(df.iloc[::10, :])
# print(df.sample(5))

# 6. Групування та статистика
# кількість людей у кожному місті
count_by_city = df.groupby('city').size().reset_index(name='count')
# print(count_by_city)
# # ТОП-5 міст
count_five_city = count_by_city.sort_values(by='count', ascending=False).head(5)
# print(count_five_city)
# # ТОП-5 email-доменів
count_by_domen = df['email_domain'].value_counts().head(5).reset_index()
# print(count_by_domen)
# # кількість унікальних доменів
count_uni_domen = df['email_domain'].nunique()
# print(count_uni_domen)

# 7. Експорт результатів
# 7.1 uk500_clean.csv	очищений датасет
df.to_csv("uk500_clean.csv", index=False, encoding='utf8')
# 7.2 gmail_users.csv	вибірка Gmail-користувачів
df_gmail['full_name'].to_csv("gmail_users.csv", index=False, encoding='utf8')
# 7.3 stats.xlsx	ТОП-міста та ТОП-домени у окремих вкладках
with pd.ExcelWriter("stats.xlsx", engine='xlsxwriter') as writer:
    count_five_city.to_excel(writer, sheet_name="TOP city", index=False)
    count_by_domen.to_excel(writer, sheet_name="TOP domens", index=False)

# count_by_city = df.groupby('city')['first_name'].count().reset_index()
# count_by_city = count_by_city.rename(columns={'first_name': 'num_people'})
# print(count_by_city)
# print("*" * 25)
# count_five_city = count_by_city.sort_values(by='num_people', ascending=False)
# count_five_city = count_five_city.head(5)
# print(count_five_city)
# print("*" * 25)
# count_five_domen = df.groupby('email_domain')['first_name'].count().reset_index()
# count_five_domen = count_five_domen.rename(columns={'first_name': 'num_people'})
# count_five_domen = count_five_domen.sort_values(by='num_people', ascending=False)
# count_uni_domen = df.groupby('email_domain').count().reset_index()
# count_five_domen = count_five_domen.head(5)
# print(count_five_domen)
# print("*" * 25)
# count_uni_domen = df['email_domain'].nunique()
# print(count_uni_domen)
# count_by_city = df.groupby('city').value_counts().reset_index()
# print(count_by_city[["city", "count"]])