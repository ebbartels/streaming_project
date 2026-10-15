import os
import snowflake.connector
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates


load_dotenv('keys/.env')

# Make sure the output folder exists (run this script from the project root)
os.makedirs('charts', exist_ok=True)

conn_params = {
    'account': 'EGGGPPA-HD66026',                      # your account identifier
    'user': 'EBARTELS',                         # your Snowflake username
    'authenticator': 'SNOWFLAKE_JWT',
    'private_key_file': 'keys/rsa_key.p8',
    'private_key_file_pwd': os.getenv('SNOWFLAKE_KEY_PASSPHRASE'),
    'warehouse': 'TRAIN_PROJECT',
    'database': 'STREAMING_PIPELINE',
    'schema': 'TRAINS',
}

conn = snowflake.connector.connect(**conn_params)
cur = conn.cursor()
cur.execute('SELECT line, total_rows FROM streaming_pipeline.trains.active_trains_by_line')
df1 = cur.fetchall()
cur.execute('SELECT service_type, COUNT(*) FROM streaming_pipeline.trains.most_recent_position GROUP BY service_type')
df2 = cur.fetchall()
cur.execute("SELECT DATE_TRUNC('minute', created_timestamp) as minute_bucket, count(DISTINCT train_id) FROM streaming_pipeline.trains.train_data_silver WHERE DAY(created_timestamp) = 15 GROUP BY ALL ORDER BY 1 DESC")
df3 = cur.fetchall()
cur.close()
conn.close()


# ---------- Chart 1: active trains by line ----------
colors = {'OR': 'Orange', 'GR': 'Green', 'RD': 'Red', 'SV': 'Silver', 'BL': 'Blue', 'YL': 'Yellow'}

lines, counts = zip(*df1)
bar_colors = [colors.get(line) for line in lines]

fig1, ax1 = plt.subplots()
bars = ax1.bar(lines, counts, color=bar_colors)
ax1.bar_label(bars, padding=3)
ax1.set_xlabel('Line')
ax1.set_ylabel('Total Count')
ax1.set_title('Total Trains by Line')
fig1.tight_layout()
fig1.savefig('charts/active_trains_by_line.png', dpi=150)


# ---------- Chart 2: trains in service over time ----------
df3 = pd.DataFrame(df3, columns=['minute', 'count'])

fig2, ax2 = plt.subplots()
ax2.plot(df3['minute'], df3['count'])
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax2.set_xlabel('Time (UTC)')
ax2.set_ylabel('Total Count')
ax2.set_title('Number of Trains Per Minute')
fig2.tight_layout()
fig2.savefig('charts/trains_in_service.png', dpi=150)


# ---------- Chart 3: service type breakdown ----------
service_colors = {'NoPassengers': 'Orange', 'Unknown': 'Red', 'Normal': 'Blue'}

types, counts = zip(*df2)
bar_colors = [service_colors.get(t, 'gray') for t in types]

fig3, ax3 = plt.subplots()
bars = ax3.bar(types, counts, color=bar_colors)
ax3.bar_label(bars, padding=3)
ax3.set_xlabel('Service type')
ax3.set_ylabel('Number of trains')
ax3.set_title('Trains by service type (latest known state)')
fig3.tight_layout()
fig3.savefig('charts/service_types.png', dpi=150)


# Opens all three windows at once. Delete this line once you're happy with the PNGs.
plt.show()