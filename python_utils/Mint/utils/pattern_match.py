import pandas as pd

df = pd.DataFrame(
   dict(
      name=['John', 'Jacob', 'Tom', 'Tim', 'Ally'],
      marks=[89, 23, 100, 56, 90],
      subjects=["Math", "Physics", "Chemistry", "Biology", "English"]
   )
)

print(df.shape[1])
for val in range(df.shape[1]):
    print(val)
exit(0)

print ("Input DataFrame is:\n", df)

regex = 'J.*'
print ("After applying ", regex, " DataFrame is:\n", df[df['name'].str.match(regex)])

regex = 'A.*'
print ("After applying ", regex, " DataFrame is:\n", df[df.name.str.match(regex)])