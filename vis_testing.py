import altair as alt
import pandas as pd

alt.renderers.enable('default')

segmented_data = pd.read_csv("segmented_data.csv", header=0)
print(segmented_data.columns)
# source = data.seattle_temps.url

alt.Chart(
    segmented_data,
    title="2010 Daily High Temperature (F) in Seattle, WA"
).mark_rect().encode(
    x='Segment ID',
    y='Activity Code',
    color=alt.Color('max(temp):Q', scale=alt.Scale(scheme="inferno")),
    tooltip=[
        alt.Tooltip('monthdate(date):T', title='Date'),
        alt.Tooltip('max(temp):Q', title='Max Temp')
    ]
).properties(width=550)
