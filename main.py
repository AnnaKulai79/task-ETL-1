import pandas as pd

URL = "https://s3-eu-west-1.amazonaws.com/shanebucket/downloads/uk-500.csv"

df = pd.read_csv(URL)

# print(df.head())
# df.info()
# print(df.describe())
# print(df.isna().sum()) #  пропущені
# print(df.duplicated().sum()) #  дублікати 
print(df.columns) 
# ['first_name', 'last_name', 'company_name', 'address', 'city', 'county','postal', 'phone1', 'phone2', 'email', 'web']
print(df.loc[5])

df['email'] = df['email'].str.lower()
df['web'] = df['web'].str.lower()
df['phone1'] = df['phone1'].str.replace(r'[^\d]+', '', regex=True)
df['phone2'] = df['phone2'].str.replace(r'[^\d]+', '', regex=True)
print(df[['address', 'city']])
df["full_name"] = df["first_name"] + " " + df["last_name"]
print(df.columns)
print(df.loc[5])
# df["email_domain"] = df["email"].str.rfind("@")