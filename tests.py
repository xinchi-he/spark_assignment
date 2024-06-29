from pytest import fixture
from pyspark.sql import SparkSession
from pyspark.sql import Row
from assignment import _build_user_sessions, _get_top_sessions, _get_top_tracks
import datetime


@fixture
def spark():
    return SparkSession.builder.appName("tests").getOrCreate()


@fixture
def play_records_df(spark):
    return spark.createDataFrame(
        [
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 0, 0),
                datetime.datetime(2024, 4, 27, 10, 0, 0)
                + datetime.timedelta(minutes=20),
                "some_track_name_1",
            ),
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 10, 0),
                datetime.datetime(2024, 4, 27, 10, 10, 0)
                + datetime.timedelta(minutes=20),
                "some_track_name_2",
            ),
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 19, 0),
                datetime.datetime(2024, 4, 27, 10, 19, 0)
                + datetime.timedelta(minutes=20),
                "some_track_name_3",
            ),
            (
                "some_userid_2",
                datetime.datetime(2024, 4, 27, 10, 19, 0),
                datetime.datetime(2024, 4, 27, 10, 19, 0)
                + datetime.timedelta(minutes=20),
                "some_track_name_3",
                # this record is not supposed to be in because no proceed track started
            ),
            (
                "some_userid_3",
                datetime.datetime(2024, 4, 27, 10, 19, 0),
                datetime.datetime(2024, 4, 27, 10, 19, 0)
                + datetime.timedelta(minutes=20),
                "some_track_name_3",
                # this record is not supposed to be in because no proceed track started
            ),
        ],
        [
            "userid",
            "timestamp",
            "session_end_ts",
            "track-name",
        ],
    )


def test_build_user_session(spark, play_records_df):
    expected_df = spark.createDataFrame(
        [
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 0, 0),
                datetime.datetime(2024, 4, 27, 10, 20, 0),
                datetime.datetime(2024, 4, 27, 10, 10, 0),
                "some_track_name_1",
                "some_track_name_2",
                "313fcb0d88f22e44e7290dcee8e020113fed8ea9",
            ),
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 0, 0),
                datetime.datetime(2024, 4, 27, 10, 20, 0),
                datetime.datetime(2024, 4, 27, 10, 19, 0),
                "some_track_name_1",
                "some_track_name_3",
                "313fcb0d88f22e44e7290dcee8e020113fed8ea9",
            ),
            (
                "some_userid_1",
                datetime.datetime(2024, 4, 27, 10, 10, 0),
                datetime.datetime(2024, 4, 27, 10, 30, 0),
                datetime.datetime(2024, 4, 27, 10, 19, 0),
                "some_track_name_2",
                "some_track_name_3",
                "95d54639fb7e3eaa5aa01425f71b8c6885921957",
            ),
        ],
        [
            "userid",
            "prev_track_start_ts",
            "session_end_ts",
            "current_track_start_ts",
            "prev_track_name",
            "curr_track_name",
            "session_id",
        ],
    )

    result_df = _build_user_sessions(play_records_df)

    assert result_df.collect() == expected_df.collect()


def test_get_top_sessions(spark, play_records_df):
    sessions_df = _build_user_sessions(play_records_df)
    result_df = _get_top_sessions(sessions_df, 50)

    expected_df = spark.createDataFrame(
        [
            (
                "313fcb0d88f22e44e7290dcee8e020113fed8ea9",
                ["some_track_name_2", "some_track_name_3"],
                [
                    Row(dist="some_track_name_2", counts=1),
                    Row(dist="some_track_name_3", counts=1),
                ],
                2,
            ),
            (
                "95d54639fb7e3eaa5aa01425f71b8c6885921957",
                ["some_track_name_3"],
                [
                    Row(dist="some_track_name_3", counts=1),
                ],
                1,
            ),
        ],
        [
            "session_id",
            "tracks_in_session",
            "track_count_map",
            "session_tracks_count",
        ],
    )

    assert result_df.collect() == expected_df.collect()


def test_get_top_tracks(spark, play_records_df):
    sessions_df = _build_user_sessions(play_records_df)
    top_sessions_df = _get_top_sessions(sessions_df, 50)
    result_df = _get_top_tracks(top_sessions_df, 10)

    expected_df = spark.createDataFrame(
        [
            (
                "313fcb0d88f22e44e7290dcee8e020113fed8ea9",
                2,
                [
                    "some_track_name_2",
                    "some_track_name_3",
                ],
            ),
            (
                "95d54639fb7e3eaa5aa01425f71b8c6885921957",
                1,
                ["some_track_name_3"],
            ),
        ],
        [
            "session_id",
            "session_tracks_count",
            "top_10_tracks",
        ],
    )

    assert result_df.collect() == expected_df.collect()
