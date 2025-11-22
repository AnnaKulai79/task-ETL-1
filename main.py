# ETL (Extract, Transform, Load)

import pandas as pd
import numpy as np

# pd.set_option("display.max_columns", 50)
# pd.set_option("display.width", 180)

URL = "https://s3-eu-west-1.amazonaws.com/shanebucket/downloads/uk-500.csv"
# 1. Імпорт та первинне дослідження
# 1.1 Завантажити uk-500.csv за допомогою pd.read_csv().
df_origin = pd.read_csv(URL)

# print(df.columns) 
# ['first_name', 'last_name', 'company_name', 'address', 'city', 'county','postal', 'phone1', 'phone2', 'email', 'web']
COLUMNS_TO_DROP = []

# 1.2 Переглянути структуру: df.head() df.info() df.describe()
# print(df.head())
# df.info()
# print(df.describe())
# print(df.describe(include=[object]).T) #  describe for str


# 1.3 Перевірити якість: 
# пропущені → df.isna().sum() 
# дублікати → df.duplicated().sum()
# print(df.isna().sum()) #  пропущені 
# print(df.isna().sum().sort_value(ascending=False).head(20))
# print(df.duplicated().sum()) #  дублікати 

# 2. Очищення даних
# 2.1 Видалити непотрібні колонки (за рішенням аналітика)
for i,col in enumerate(df_origin.columns):
    print(f"{i + 1:02d}. {col}")
df = df_origin.copy()

if COLUMNS_TO_DROP:
    df = df.drop(columns=[i for i in COLUMNS_TO_DROP if i in df.columns], errors="ignore")
else:
    print("\n COLUMNS_TO_DROP = []")

def standartize_text(s):
    if pd.isna(s):
        return np.nan
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    s = " ".join(s.split())
    return s

# 2.2 Привести email та web до нижнього регістру.
possible_email_cols = [c for c in df.columns if "email" in c.lower()]
possible_web_cols = [c for c in df.columns if ("web" in c.lower() or "url" in c.lower())]

for col in possible_email_cols:
    df[col] = df[col].str.lower()
for col in possible_web_cols:
    df[col] = df[col].str.lower()

# 2.3 Очистити phone та fax від пробілів/символів.
df['phone1'] = df['phone1'].str.replace(r'[^\d]+', '', regex=True)
df['phone2'] = df['phone2'].str.replace(r'[^\d]+', '', regex=True)

# 2.4 Стандартизувати формат текстових полів.
for col in df.select_dtypes(include=['object']).columns:
    df[col] = df[col].apply(standartize_text)


# 3. Створення нових колонок (Feature Engineering)
# 3.1 full_name	Ім’я + прізвище
df.insert(loc=2, column="full_name", value=(df["first_name"] + " " + df["last_name"])) # df["full_name"] = df["first_name"] + " " + df["last_name"]
# print(df.columns)

# 3.2 email_domain	Домен email
df["email_domain"] = df["email"].str.split("@").str[-1]

# 3.3 city_length	Довжина назви міста
df["city_length"] = df["city"].str.len()

# 3.4 is_gmail	Boolean: чи email з gmail.com
df['is_gmail'] = df['email_domain'] == 'gmail.com'

# 4. Фільтрація даних Створити підвибірки:
# 4.1 користувачі з доменом gmail.com
df_gmail = df.loc[df['email_domain'] == 'gmail.com']
# print(df_gmail[['full_name']])
# 4.2 працівники компаній з “LLC” або “Ltd”
df_llc_ltd = df.loc[df['company_name'].str.contains('LLC|Ltd', case=False, na=False, regex=True)]
# print(df_llc_ltd[['full_name', 'company_name']])
# 4.3 люди з міста London
df_london = df.loc[df['county'] == 'London']
# print(df_london)
# # 4.4 компанії з назвою ≥ 4 слів
df_more_four = df.loc[df['company_name'].str.split().str.len() >= 4]
# print(df_more_four[['full_name', 'company_name']])

# 5. Позиційна вибірка (iloc)
# 5.1 Перші 10 рядків + колонки 2–5
# print(df.iloc[:10, 1:5])

# 5.2 Кожний 10-й рядок
# print(df.iloc[::10, :])

# 5.3 5 випадкових рядків → .sample(5)
# print(df.sample(5))

# 6. Групування та статистика
# 6.1 кількість людей у кожному місті
count_by_city = df.groupby('city').size().reset_index(name='count')
# print(count_by_city)
# 6.2 ТОП-5 міст
count_five_city = count_by_city.sort_values(by='count', ascending=False).head(5)
# print(count_five_city)
# 6.3 ТОП-5 email-доменів
count_by_domen = df['email_domain'].value_counts().head(5).reset_index()
# print(count_by_domen)
# 6.4 кількість унікальних доменів
count_uni_domen = df['email_domain'].nunique()
# print(count_uni_domen)

agg_by_city = df.groupby("city").agg(
    people_count=("first_name", "count"),
    uniq_domain=("email_domain", "nunique")
 ).sort_values("people_count",ascending=False).head(10)
print(agg_by_city)
city_count = df.groupby("city")["first_name"].nunique()
print(city_count)

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