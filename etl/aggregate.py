"""Build hourly aggregates and event transitions from event chunks."""
from collections import defaultdict
import pandas as pd


class Aggregator:
    """Accumulates partial aggregates across chunks using vectorized operations."""

    def __init__(self, events_list=None, event_name_mapping=None, event_package_mapping=None):
        # Hourly aggregates: {(hour, event_name, package_name, os): {'events': int, 'sessions': set(), 'users': set()}}
        self.hourly = defaultdict(lambda: {
            'events': 0,
            'sessions': set(),
            'users': set()
        })

        # Event transitions: {(hour, from_event, to_event): count}
        self.transitions = defaultdict(int)

        # Track last event per session for cross-chunk transitions
        self.session_tails = {}

        # event_name -> [package_names]: drives filtering and zero-fill
        self.event_package_mapping = event_package_mapping or {}


    def process_chunk(self, df):
        """Process a chunk of events using vectorized operations."""
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.floor('h')

        # --- Vectorized hourly aggregates ---
        grouped = df.groupby(['hour', 'event_name', 'package_name', 'os'])

        for (hour, event_name, package_name, os), group in grouped:
            key = (hour, event_name, package_name, os)
            self.hourly[key]['events'] += len(group)
            self.hourly[key]['sessions'].update(group['session_id'])
            self.hourly[key]['users'].update(group['user_id'])

        # --- Transitions ---
        pdf = df[['session_id', 'timestamp', 'hour', 'event_name']].sort_values(
            ['session_id', 'timestamp']
        )
        pdf['prev_event'] = pdf.groupby('session_id')['event_name'].shift(1)
        pdf['prev_hour'] = pdf.groupby('session_id')['hour'].shift(1)

        trans_df = pdf.dropna(subset=['prev_event'])
        for _, row in trans_df.iterrows():
            self.transitions[(row['prev_hour'], row['prev_event'], row['event_name'])] += 1

        # --- Handle cross-chunk transitions ---
        session_first = df.sort_values('timestamp').groupby('session_id').first()
        session_last = df.sort_values('timestamp').groupby('session_id').last()

        for session_id in session_first.index:
            if session_id in self.session_tails:
                prev_ts, prev_event, prev_hour = self.session_tails[session_id]
                first_event = session_first.loc[session_id]
                self.transitions[(prev_hour, prev_event, first_event['event_name'])] += 1

            last_event = session_last.loc[session_id]
            self.session_tails[session_id] = (
                last_event['timestamp'],
                last_event['event_name'],
                last_event['hour']
            )

    def to_dataframes(self):
        """Convert aggregates to DataFrames."""
        # Hourly counts
        hourly_rows = []
        for (hour, event_name, package_name, os), counts in self.hourly.items():
            date = hour.date() if hasattr(hour, 'date') else pd.to_datetime(hour).date()
            hour_of_day = hour.hour if hasattr(hour, 'hour') else pd.to_datetime(hour).hour

            hourly_rows.append({
                'date': date,
                'hour_of_day': hour_of_day,
                'event_name': event_name,
                'package_name': package_name,
                'os': os,
                'event_count': counts['events'],
                'session_count': len(counts['sessions']),
                'user_count': len(counts['users'])
            })

        hourly_df = pd.DataFrame(hourly_rows)

        # Zero-fill: ensure every (date, hour_of_day, os, event_name, package_name) combo appears
        if self.event_package_mapping and not hourly_df.empty:
            unique_slots = hourly_df[['date', 'hour_of_day', 'os']].drop_duplicates()

            complete_rows = []
            for _, slot in unique_slots.iterrows():
                for event_name, packages in self.event_package_mapping.items():
                    for package_name in packages:
                        complete_rows.append({
                            'date': slot['date'],
                            'hour_of_day': slot['hour_of_day'],
                            'os': slot['os'],
                            'event_name': event_name,
                            'package_name': package_name,
                        })

            complete_df = pd.DataFrame(complete_rows).drop_duplicates()

            hourly_df = complete_df.merge(
                hourly_df,
                on=['date', 'hour_of_day', 'os', 'event_name', 'package_name'],
                how='left'
            ).fillna({'event_count': 0, 'session_count': 0, 'user_count': 0})

            hourly_df['event_count'] = hourly_df['event_count'].astype(int)
            hourly_df['session_count'] = hourly_df['session_count'].astype(int)
            hourly_df['user_count'] = hourly_df['user_count'].astype(int)

        if not hourly_df.empty:
            hourly_df = hourly_df.sort_values(['date', 'hour_of_day', 'event_name', 'package_name', 'os'])

        # Transitions
        trans_rows = []
        for (hour, from_event, to_event), count in self.transitions.items():
            date = hour.date() if hasattr(hour, 'date') else pd.to_datetime(hour).date()
            hour_of_day = hour.hour if hasattr(hour, 'hour') else pd.to_datetime(hour).hour

            trans_rows.append({
                'date': date,
                'hour_of_day': hour_of_day,
                'from_event': from_event,
                'to_event': to_event,
                'transition_count': count
            })

        trans_df = pd.DataFrame(trans_rows)
        if not trans_df.empty:
            trans_df = trans_df.sort_values(['date', 'hour_of_day', 'from_event', 'to_event'])

        return hourly_df, trans_df
