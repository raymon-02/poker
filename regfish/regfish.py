import abc
import argparse
import json
import logging
import os
from collections import defaultdict, Counter, OrderedDict
from datetime import timezone, datetime, timedelta
from enum import Enum
from shutil import copyfile
from types import SimpleNamespace
from typing import NamedTuple

FORMAT = "%(asctime)-15s [%(levelname)8s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

INDEX_FILE_NAME = "index.txt"
PLAYER_FILE_NAME = "players.txt"
STAT_FILE_NAME = "stats.txt"
FIRST_HAND_FILE_NAME = "firsthand-{}.txt"
BU_FOLDS_FILE_NAME = "bufolds-{}.txt"

EXPRESSO_NITRO = "Expresso Nitro"
LIMIT_SUMMARY = "limit_summary"
WNX_CM = "WNX.cm"
DATE_FORMAT = "%Y/%m/%d"
DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"
REF_FISH_FOLDER_FORMAT = "reg-{}-fish-{}"

WEEKDAY = "Weekday"
WEEKEND = "Weekend"
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", WEEKDAY, "Saturday", "Sunday", WEEKEND]
WEEKENDS = {"Saturday", "Sunday"}

HANDS_IN_FILE = 10000


class Mode(Enum):
    INDEX = "index"
    FULL = "full"
    FAST = "fast"


class CalcMode(Enum):
    TABLES = "tables"
    HANDS = "hands"


class TableFileMeta(NamedTuple):
    id: int
    tsdata_file: str
    data_files: set[str]


class TableData(NamedTuple):
    timestamp: str
    prize_pool: float
    buy_in: float


class Player(NamedTuple):
    nickname: str
    hands: int
    seat: int


class EliminatedType(Enum):
    ME = "me"
    OTHER = "other"
    BOTH = "both"


class XA(NamedTuple):
    nickname: str
    after_hand: int
    eliminated_by: EliminatedType


NO_XA = XA("", -1, EliminatedType.OTHER)


class XAType(Enum):
    FISH = "fish"
    LOST = "lost"


class IndexedPlayer(NamedTuple):
    nickname: str
    tables: int
    hands: int


class PlayerType(Enum):
    REG = "Reg"
    FISH = "Fish"


class TableFast(NamedTuple):
    id: int
    table_data: TableData
    players: list[Player]


class Table(NamedTuple):
    id: int
    table_data: TableData
    seat: int
    players: list[Player]
    xa: XA
    lost_after_hand: int


class TableStat(NamedTuple):
    file_meta: TableFileMeta
    buy_in_mp: str
    xa_after_hand: int
    eliminated_by: EliminatedType
    lost_after_hand: int
    is_reg_left: bool


class Report(NamedTuple):
    days: OrderedDict[str, Counter[tuple[int, int]]]
    hours: OrderedDict[int, Counter[tuple[int, int]]]
    threes: OrderedDict[int, Counter[tuple[int, int]]]
    day_hours: OrderedDict[tuple[str, int], Counter[tuple[int, int]]]
    day_threes: OrderedDict[tuple[str, int], Counter[tuple[int, int]]]
    weeks: OrderedDict[int, Counter[tuple[int, int]]]


class IndexCreator:

    def __init__(self, data, tsdata, result, nicknames):
        self.path_data = data
        self.path_tsdata = tsdata
        self.path_result = result
        self.self_nicknames = nicknames
        self.index_file = os.path.join(result, INDEX_FILE_NAME)
        self.player_file = os.path.join(result, PLAYER_FILE_NAME)

    def index(self):
        logging.info("Run indexing")
        files_to_index = self.__get_files_to_index__()
        tables = self.__get_tables__(files_to_index)
        indexed_players = IndexCreator.__get_indexed_players__(tables)
        self.__write_index__(tables, indexed_players)
        logging.info("Finish indexing")

    def __get_files_to_index__(self):
        logging.info("Start getting files to index")
        data_file_idx = defaultdict(set)
        tsdata_file_idx = {}
        count = 0
        for root, _, files in os.walk(self.path_data):
            for file in filter(is_data_file, files):
                table_id = parse_table_id(file)
                data_file_idx[table_id].add(os.path.join(root, file))
                count += 1
        logging.info("Data files to index: {} ".format(count))

        count = 0
        for root, _, files in os.walk(self.path_tsdata):
            for file in filter(is_tsdata_file, files):
                table_id = parse_table_id(file)
                tsdata_file_idx[table_id] = os.path.join(root, file)
                count += 1
        logging.info("TS data files to index: {} ".format(count))

        result = []
        for table_id, tsdata_file in tsdata_file_idx.items():
            data_files = data_file_idx[table_id]
            if not data_files:
                logging.warning("No data files for table id {}. Skipping {}".format(table_id, tsdata_file))
                continue
            result.append(TableFileMeta(table_id, tsdata_file, data_files))

        logging.info("Total file entities to index: {}".format(len(result)))

        return result

    def __get_tables__(self, files_to_index):
        logging.info("Start getting tables from {} files".format(len(files_to_index)))
        tables = []
        mod = statistic_mod(len(files_to_index))
        for i, file_to_index in enumerate(files_to_index):
            if i % mod == 0:
                logging.info("Getting table from {}/{} files...".format(i, len(files_to_index)))
            table_data, won = IndexCreator.__get_table_data__(file_to_index.tsdata_file)
            if not table_data:
                logging.warning("Cannot get table data from TS file. Skipping: {}".format(file_to_index.tsdata_file))
                continue
            seat, players, xa, hand_count = self.__get_data__(file_to_index.data_files)
            lost_after_hand = 0 if won else hand_count
            tables.append(Table(file_to_index.id, table_data, seat, players, xa, lost_after_hand))

        logging.info("Got {} tables from files".format(len(tables)))

        return tables

    @staticmethod
    def __get_indexed_players__(tables):
        logging.info("Start getting indexed players")
        indexed_players = {}
        player_tables = Counter()
        player_hands = Counter()
        for table in tables:
            for player in table.players:
                player_tables[player.nickname] += 1
                player_hands[player.nickname] += player.hands
        for nickname, table_count in player_tables.items():
            indexed_player = IndexedPlayer(nickname, table_count, player_hands[nickname])
            indexed_players[nickname] = indexed_player

        logging.info("Got {} indexed players".format(len(indexed_players)))

        return list(indexed_players.values())

    @staticmethod
    def __get_table_data__(tsdata_file):
        with open(tsdata_file, encoding="utf-8") as handler:
            lines = handler.readlines()
        buy_in, prize_pool, timestamp, won = None, None, None, False
        for line in lines:
            if line.startswith("Buy-In"):
                buy_in = parse_buy_in(line)
            elif line.startswith("Prizepool"):
                prize_pool = parse_prize_pool(line)
            elif line.startswith("Tournament started"):
                timestamp = parse_tournament_started(line)
            elif line.startswith("You won"):
                won = True

        if buy_in is None or prize_pool is None or timestamp is None:
            return None, won

        return TableData(timestamp, prize_pool, buy_in), won

    def __get_data__(self, data_files):
        xa = None
        seats = {}
        counter = Counter()
        lines = []
        for data_file in sorted_data_files(data_files):
            with open(data_file, encoding="utf-8") as handler:
                lines.extend(handler.readlines())

        i = 0
        hand_count = 0
        prev_hand_nicknames = set()
        while i < len(lines):
            while i < len(lines) and not bool(lines[i].strip()):
                i += 1
            while i < len(lines) and not lines[i].startswith("Seat"):
                i += 1
            xa_nicknames = set()
            while i < len(lines) and lines[i].startswith("Seat"):
                seat, nickname = parse_seat_nickname(lines[i])
                if len(seats) < 3:
                    seats[nickname] = seat
                if nickname not in self.self_nicknames:
                    counter[nickname] += 1
                xa_nicknames.add(nickname)
                i += 1
            xa_inter = xa_nicknames.intersection(self.self_nicknames)
            if xa is None and len(xa_inter) == 1 and len(xa_nicknames) == 2:
                nickname = next(iter(xa_nicknames.difference(xa_inter)))
                eliminated_by_nicknames = xa_nicknames.intersection(prev_hand_nicknames)
                eliminated_by = EliminatedType.OTHER
                if len(eliminated_by_nicknames) > 1:
                    eliminated_by = EliminatedType.BOTH
                elif eliminated_by_nicknames.intersection(self.self_nicknames):
                    eliminated_by = EliminatedType.ME
                xa = XA(nickname, hand_count, eliminated_by)
            while i < len(lines) and not lines[i].startswith("Seat"):
                i += 1
            prev_hand_nicknames = set()
            while i < len(lines) and lines[i].startswith("Seat"):
                nickname = parse_nickname_summary(lines[i])
                prev_hand_nicknames.add(nickname)
                i += 1
            while i < len(lines) and bool(lines[i].strip()):
                i += 1
            hand_count += 1

        seat = seats[next(iter(self.self_nicknames.intersection(seats)))]
        players = [Player(nickname, count, seats[nickname]) for nickname, count in counter.items()]

        return seat, players, xa if xa else NO_XA, hand_count

    def __write_index__(self, tables, indexed_players):
        logging.info("Saving {} tables into index file...".format(len(tables)))
        serialized_tables = map(serialize_table, tables)
        mod = statistic_mod(len(tables))
        with open(self.index_file, "w", encoding="utf-8") as handler:
            for i, serialized_table in enumerate(serialized_tables):
                if i % mod == 0:
                    logging.info("Adding {}/{} table into index file...".format(i, len(tables)))
                handler.write(serialized_table)
                handler.write("\n")
        logging.info("Saved tables into index file")

        logging.info("Saving {} indexed players into players file...".format(len(indexed_players)))
        with open(self.player_file, "w", encoding="utf-8") as handler:
            for indexed_player in indexed_players:
                handler.write(serialize_indexed_player(indexed_player))
                handler.write("\n")
        logging.info("Saved indexed players into players file")


class AbstractStatisticCalculator(abc.ABC):

    def __init__(self, calcdata, calctsdata, colormarkers, result, original_interval, current_interval):
        self.path_calc_data = calcdata
        self.path_calc_tsdata = calctsdata
        self.path_colormarkers = colormarkers
        self.path_result = result
        self.original_interval = original_interval
        self.current_interval = current_interval
        self.index_file = os.path.join(result, INDEX_FILE_NAME)
        self.player_file = os.path.join(result, PLAYER_FILE_NAME)
        self.index = self.__get_index__()
        self.players = self.__get_players__()

    def __get_index__(self):
        logging.info("Loading index...")
        tables = {}
        with open(self.index_file, encoding="utf-8") as handler:
            for line in handler:
                table = deserialize_table(line)
                tables[table.id] = table

        logging.info("Loaded index with {} tables".format(len(tables)))

        return tables

    def __get_players__(self):
        logging.info("Loading players...")
        players = {}
        with open(self.player_file, encoding="utf-8") as handler:
            for line in handler:
                player = deserialized_indexed_player(line)
                players[player.nickname] = player

        logging.info("Loaded {} players".format(len(players)))

        return players

    def __get_colored_players__(self):
        logging.info("Start getting colored players")
        colored_players = {}
        for root, _, files in os.walk(self.path_colormarkers):
            for file in filter(is_color_marker_file, files):
                filename = os.path.join(root, file)
                with open(filename, encoding="utf-8") as handler:
                    text = "".join(handler.readlines())
                    player = json.loads(text, object_hook=lambda d: SimpleNamespace(**d))
                    player_type = PlayerType.REG if player.ColorMarker.IsReg else PlayerType.FISH
                    colored_players[player.Player.Nickname] = player_type

        logging.info("Got {} colored players".format(len(colored_players)))

        return colored_players

    def __is_reg__(self, nickname, colored_players, calcmode, regtables, reghands):
        color_status = colored_players.get(nickname, None)
        if color_status is not None:
            return color_status == PlayerType.REG
        player = self.players.get(nickname, None)
        if player is None:
            return False
        if calcmode == CalcMode.TABLES:
            return player.tables >= regtables
        else:
            return player.hands >= reghands

    def __get_stat_lines_header__(self, calcmode, regtables, reghands, interval, buyin):
        header_lines = []
        current_time = datetime.now().strftime(DATETIME_FORMAT)
        header_lines.append("Run time:             {}".format(current_time))
        header_lines.append("Reg calculation mode: {}".format(calcmode.name))
        header_lines.append("Reg table value:      {}".format(regtables))
        header_lines.append("Reg hand value:       {}".format(reghands))
        header_lines.append("In original interval: {}".format(self.original_interval))
        header_lines.append("In interval:          {}".format(self.current_interval))
        header_lines.append("In interval UTC:      {}".format(interval))
        header_lines.append("Buy-in:               {}".format(buyin if buyin else "all"))
        header_lines.append("")
        header_lines.append("")

        return header_lines

    @staticmethod
    def __get_stat_lines__(table_stats, last_table_stats=None, report=None, map_f=lambda el: el):
        reduced_stat_lines, stat_lines = [], []
        keys = sorted(table_stats.keys())
        AbstractStatisticCalculator.__get_stat_counter_lines__(
            table_stats, "tables", keys, stat_lines, map_f=map_f
        )

        if last_table_stats:
            AbstractStatisticCalculator.__get_stat_counter_lines__(
                last_table_stats, "last tables", keys, stat_lines
            )

        reduced_stat_lines.extend(stat_lines)

        if not report:
            return reduced_stat_lines, stat_lines

        stat_lines.append("")
        stat_lines.append("BY DAYS")
        for day_name, counter in report.days.items():
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, day_name, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY HOURS")
        for hour, counter in report.hours.items():
            hour_range = "{}-{}".format(hour, hour + 1)
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, hour_range, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY THREE HOURS")
        for hour, counter in report.threes.items():
            hour_range = "{}-{}".format(hour, hour + 3)
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, hour_range, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY DAYS/HOURS")
        for (day_name, hour), counter in report.day_hours.items():
            day_hour_range = "{} {}-{}".format(day_name, hour, hour + 1)
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, day_hour_range, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY DAYS/THREE HOURS")
        for (day_name, hour), counter in report.day_threes.items():
            day_hour_range = "{} {}-{}".format(day_name, hour, hour + 3)
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, day_hour_range, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY WEEKS")
        for week, counter in report.weeks.items():
            AbstractStatisticCalculator.__get_stat_counter_lines__(counter, str(week), keys, stat_lines)

        return reduced_stat_lines, stat_lines

    @staticmethod
    def __get_stat_counter_lines__(counter, total_name, keys, lines, map_f=lambda el: el):
        total = sum(map(map_f, counter.values()))
        lines.append("Total {}: {}".format(total_name, total))
        for key in keys:
            count = map_f(counter[key])
            reg, fish = key
            per = count / total * 100 if total > 0 else 0
            lines.append("Reg: {}, Fish: {}, count: {:7}, rate: {:0.3f}%".format(reg, fish, count, per))
        lines.append("")


class FullStatisticCalculator(AbstractStatisticCalculator):

    def __init__(self, calcdata, calctsdata, colormarkers, result, original_interval, current_interval):
        super().__init__(calcdata, calctsdata, colormarkers, result, original_interval, current_interval)

    def calculate(
            self, calcmode, regtables, reghands, interval, buyin, is_sort, buyinsort, is_xa, is_first_hand, is_bu_fold
    ):
        logging.info("Run FULL stat calculation")
        files_to_calculate = self.__get_calc_files__()
        colored_players = self.__get_colored_players__()
        table_stats, report = self.__get_stats__(
            files_to_calculate, calcmode, colored_players, regtables, reghands, interval, buyin, buyinsort
        )
        header_lines = self.__get_stat_lines_header__(calcmode, regtables, reghands, interval, buyin)
        reduced_stat_lines, stat_lines = AbstractStatisticCalculator.__get_stat_lines__(
            table_stats, report=report, map_f=len
        )
        path_result_run = self.__generate_result_run_folder__()
        FullStatisticCalculator.__write_stats__(header_lines, stat_lines, path_result_run)
        if is_sort:
            FullStatisticCalculator.__copy_data_files__(table_stats, path_result_run)
        if is_xa:
            FullStatisticCalculator.__copy_xa_data_files__(table_stats, path_result_run)
        if is_first_hand:
            FullStatisticCalculator.__copy_first_hands__(table_stats, path_result_run)
        if is_bu_fold:
            FullStatisticCalculator.__copy_bu_folds_hands__(table_stats, path_result_run)
        logging.info("Finish FULL stat calculation")

        return reduced_stat_lines

    def __get_calc_files__(self):
        logging.info("Start getting files to calculate stat")
        data_file_idx = defaultdict(set)
        tsdata_file_idx = {}
        count = 0
        for root, _, files in os.walk(self.path_calc_data):
            for file in filter(is_data_file, files):
                table_id = parse_table_id(file)
                data_file_idx[table_id].add(os.path.join(root, file))
                count += 1
        logging.info("Data files to calculate stat: {} ".format(count))

        count = 0
        for root, _, files in os.walk(self.path_calc_tsdata):
            for file in filter(is_tsdata_file, files):
                table_id = parse_table_id(file)
                tsdata_file_idx[table_id] = os.path.join(root, file)
                count += 1
        logging.info("TS data files to calculate stat: {} ".format(count))

        result = []
        for table_id, tsdata_file in tsdata_file_idx.items():
            data_files = data_file_idx[table_id]
            if not data_files:
                logging.warning("No data files for table id {}. Skipping {}".format(table_id, tsdata_file))
                continue
            result.append(TableFileMeta(table_id, tsdata_file, data_files))

        logging.info("Total file entities to calculate stat: {}".format(len(result)))

        return result

    def __get_stats__(
            self, files_to_calculate, calcmode, colored_players, regtables, reghands, interval, buyin, buyinsort
    ):
        logging.info("Start getting table stats")

        table_stats = defaultdict(list)
        filter_count = 0
        filter_out_count = 0
        not_found_count = 0
        filters = [interval_filter(interval)] + ([] if buyin is None else [buyin_filter(buyin)])
        regs = set()
        report = FullStatisticCalculator.__generate_report_keys__()

        for file_to_calc in files_to_calculate:
            table = self.index.get(file_to_calc.id, None)
            if table is None:
                not_found_count += 1
                continue
            if not all(map(lambda f: f(table), filters)):
                filter_out_count += 1
                continue

            reg = 0
            fish = 0
            reg_player = None
            for player in table.players:
                if player.nickname in regs:
                    reg_player = player
                    reg += 1
                elif self.__is_reg__(player.nickname, colored_players, calcmode, regtables, reghands):
                    regs.add(player.nickname)
                    reg_player = player
                    reg += 1
                else:
                    fish += 1
            key = (reg, fish)
            xa_after_hand = -1 if table.xa.nickname in regs else table.xa.after_hand
            table_stats[key].append(
                TableStat(
                    file_to_calc,
                    get_buy_in_mp(table, buyinsort),
                    xa_after_hand,
                    table.xa.eliminated_by,
                    table.lost_after_hand,
                    is_reg_left(table, reg_player, key)
                )
            )
            FullStatisticCalculator.__add_to_report__(report, table, key)
            filter_count += 1

        logging.info("Tables to calculate stat: {}".format(filter_count))
        logging.info("Tables were filtered out: {}".format(filter_out_count))
        if not_found_count > 0:
            logging.warning("Tables were not found: {}. Needs to rerun index with calc files".format(not_found_count))
        logging.info("Got {} buckets in table stats: {}".format(len(table_stats), list(table_stats.keys())))

        return table_stats, report

    @staticmethod
    def __generate_report_keys__():
        days = OrderedDict()
        day_hours = OrderedDict()
        day_threes = OrderedDict()
        for day_name in DAY_NAMES:
            days[day_name] = Counter()
            for hour in range(24):
                day_hours[(day_name, hour)] = Counter()
            for hour in range(0, 24, 3):
                day_threes[(day_name, hour)] = Counter()

        hours = OrderedDict()
        for hour in range(24):
            hours[hour] = Counter()

        threes = OrderedDict()
        for hour in range(0, 24, 3):
            threes[hour] = Counter()

        weeks = OrderedDict()
        for week in range(1, 5):
            weeks[week] = Counter()

        return Report(days, hours, threes, day_hours, day_threes, weeks)

    @staticmethod
    def __add_to_report__(report, table, key):
        timestamp_utc = datetime.strptime(table.table_data.timestamp, DATETIME_FORMAT).replace(tzinfo=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        table_timestamp = timestamp_utc.astimezone(local_tz)
        day_name = table_timestamp.strftime('%A')
        hour = table_timestamp.hour
        three_hour = table_timestamp.hour // 3 * 3
        week = table_timestamp.day // 7 + int(bool(table_timestamp.day % 7))
        week = 4 if week > 4 else week

        report.days[day_name][key] += 1
        report.day_hours[(day_name, hour)][key] += 1
        report.day_threes[(day_name, three_hour)][key] += 1
        if day_name in WEEKENDS:
            report.days[WEEKEND][key] += 1
            report.day_hours[(WEEKEND, hour)][key] += 1
            report.day_threes[(WEEKEND, three_hour)][key] += 1
        else:
            report.days[WEEKDAY][key] += 1
            report.day_hours[(WEEKDAY, hour)][key] += 1
            report.day_threes[(WEEKDAY, three_hour)][key] += 1
        report.hours[hour][key] += 1
        report.threes[three_hour][key] += 1
        report.weeks[week][key] += 1

    def __generate_result_run_folder__(self):
        logging.info("Creating result folder...")
        date = datetime.now().strftime("%Y-%m-%d-%H%M%S%f")
        result_run_folder = os.path.join(self.path_result, "result-run-{}".format(date))
        os.makedirs(result_run_folder)
        logging.info("Created result folder: {}".format(result_run_folder))
        return result_run_folder

    @staticmethod
    def __write_stats__(header_lines, stat_lines, path_result_run):
        logging.info("Creating stat file...")
        filename = os.path.join(path_result_run, STAT_FILE_NAME)
        with open(filename, "w", encoding="utf-8") as handler:
            for line in header_lines:
                handler.write(line)
                handler.write("\n")
            for line in stat_lines:
                handler.write(line)
                handler.write("\n")
        logging.info("Created stat file")

    @staticmethod
    def __copy_data_files__(table_stats, path_result_run):
        logging.info("Copying data files...")
        for (reg, fish), table_stat_buket in table_stats.items():
            logging.info("Copying data files: reg={}, fish={}...".format(reg, fish))
            folder_name = REF_FISH_FOLDER_FORMAT.format(reg, fish)
            mod = statistic_mod(len(table_stat_buket))
            for i, table_stat in enumerate(table_stat_buket):
                if i % mod == 0:
                    logging.info("Copying data files {}/{}...".format(i, len(table_stat_buket)))
                path_data = os.path.join(path_result_run, folder_name, table_stat.buy_in_mp, "data")
                path_tsdata = os.path.join(path_result_run, folder_name, table_stat.buy_in_mp, "tsdata")
                os.makedirs(path_data, exist_ok=True)
                os.makedirs(path_tsdata, exist_ok=True)
                copy_tsdata_file = os.path.join(path_tsdata, table_stat.file_meta.tsdata_file.split(os.sep)[-1])
                copyfile(table_stat.file_meta.tsdata_file, copy_tsdata_file)
                for data_file in table_stat.file_meta.data_files:
                    copy_data_file = os.path.join(path_data, data_file.split(os.sep)[-1])
                    copyfile(data_file, copy_data_file)
            logging.info("Copied data files: reg={}, fish={}".format(reg, fish))

        logging.info("Copied data files")

    @staticmethod
    def __copy_xa_data_files__(table_stats, path_result_run):
        logging.info("Copying data files for XA...")
        xa_fish_filter_out_count, xa_lost_filter_out_count = 0, 0
        for (reg, fish), table_stat_buket in table_stats.items():
            logging.info("Copying data files for XA: reg={}, fish={}...".format(reg, fish))
            folder_name = REF_FISH_FOLDER_FORMAT.format(reg, fish)
            xa_folder_name = os.path.join(path_result_run, folder_name, "xa")
            is_one_one = reg == 1 and fish == 1

            table_stats_with_xa = list(filter(xa_filter, table_stat_buket))
            xa_fish_filter_out_count += len(table_stat_buket) - len(table_stats_with_xa)
            FullStatisticCalculator.__copy_xa_type_data_files__(
                table_stats_with_xa, xa_folder_name, is_one_one, XAType.FISH
            )

            table_stats_with_lost = list(filter(xa_lost_filter, table_stat_buket))
            xa_lost_filter_out_count += len(table_stat_buket) - len(table_stats_with_lost)
            FullStatisticCalculator.__copy_xa_type_data_files__(
                table_stats_with_lost, xa_folder_name, is_one_one, XAType.LOST
            )

            logging.info("Copied data files for XA: reg={}, fish={}".format(reg, fish))

        logging.info("File entries for XA fish were filtered out: {}".format(xa_fish_filter_out_count))
        logging.info("File entries for XA lost were filtered out: {}".format(xa_lost_filter_out_count))
        logging.info("Copied data files for XA")

    @staticmethod
    def __copy_xa_type_data_files__(table_stat_buket, xa_folder_name, is_one_one, xa_type):
        mod = statistic_mod(len(table_stat_buket))
        for i, table_stat in enumerate(table_stat_buket):
            if is_one_one:
                reg_folder_name = "regleft" if table_stat.is_reg_left else "regright"
                folder_name = os.path.join(xa_folder_name, reg_folder_name, xa_type.value)
            else:
                folder_name = os.path.join(xa_folder_name, xa_type.value)
            folder_bucket_name = get_folder_bucket_name(table_stat, xa_type)
            path_data = os.path.join(folder_name, folder_bucket_name, "data")
            path_tsdata = os.path.join(folder_name, folder_bucket_name, "tsdata")
            os.makedirs(path_data, exist_ok=True)
            os.makedirs(path_tsdata, exist_ok=True)
            if i % mod == 0:
                logging.info("Copying data files for XA '{}' {}/{}...".format(xa_type.value, i, len(table_stat_buket)))
            copy_tsdata_file = os.path.join(path_tsdata, table_stat.file_meta.tsdata_file.split(os.sep)[-1])
            copyfile(table_stat.file_meta.tsdata_file, copy_tsdata_file)
            for data_file in table_stat.file_meta.data_files:
                copy_data_file = os.path.join(path_data, data_file.split(os.sep)[-1])
                copyfile(data_file, copy_data_file)

    @staticmethod
    def __copy_first_hands__(table_stats, path_result_run):
        logging.info("Copying first hands into files...")
        path_data = os.path.join(path_result_run, "firsthands")
        path_data_file_name = os.path.join(path_data, FIRST_HAND_FILE_NAME)
        os.makedirs(path_data, exist_ok=True)

        data_files = []
        for table_stat_buket in table_stats.values():
            for i, table_stat in enumerate(table_stat_buket):
                data_file = list(sorted_data_files(table_stat.file_meta.data_files))[0]
                data_files.append(data_file)

        steps = len(data_files) // HANDS_IN_FILE
        zfill_len = len(str(steps + 1))
        for step in range(steps + 1):
            start = step * HANDS_IN_FILE
            end = min((step + 1) * HANDS_IN_FILE, len(data_files))
            file_name = path_data_file_name.format(str(step).zfill(zfill_len))
            logging.info("Copying first hands {}-{}...".format(start, end - 1))
            with open(file_name, "w", encoding="utf-8") as handler:
                for i in range(start, end):
                    with open(data_files[i], encoding="utf-8") as handler_read:
                        lines = handler_read.readlines()
                    i = 0
                    while i < len(lines) and not bool(lines[i].strip()):
                        i += 1
                    si = i
                    while i < len(lines) and bool(lines[i].strip()):
                        i += 1
                    handler.writelines(lines[si:i])
                    handler.write("\n")

        logging.info("Copied {} first hands into files".format(len(data_files)))

    @staticmethod
    def __copy_bu_folds_hands__(table_stats, path_result_run):
        logging.info("Copying bu folds hands into files...")
        path_data = os.path.join(path_result_run, "bufolds")
        path_data_file_name = os.path.join(path_data, BU_FOLDS_FILE_NAME)
        os.makedirs(path_data, exist_ok=True)

        data_files = []
        for table_stat_buket in table_stats.values():
            for i, table_stat in enumerate(table_stat_buket):
                data_files.extend(sorted_data_files(table_stat.file_meta.data_files))

        bu_folds_hands_copied = 0
        bu_folds_hands_i = 0
        step = 0
        zfill_len = len(str(len(data_files) // HANDS_IN_FILE * 20))
        file_name = path_data_file_name.format(str(step).zfill(zfill_len))
        handler = open(file_name, "w", encoding="utf-8")
        mod = statistic_mod(len(data_files))
        for j, data_file in enumerate(data_files):
            if j % mod == 0:
                logging.info("Looking for bu folds hands in files {}/{}...".format(j, len(data_files)))
            with open(data_file, encoding="utf-8") as handler_read:
                lines = handler_read.readlines()
            i = 0
            while i < len(lines):
                while i < len(lines) and not bool(lines[i].strip()):
                    i += 1
                si = i
                seats = 0
                bu_folds_hand = False
                while i < len(lines) and not lines[i].startswith("Seat"):
                    i += 1
                while i < len(lines) and lines[i].startswith("Seat"):
                    i += 1
                    seats += 1
                while i < len(lines) and bool(lines[i].strip()):
                    if i + 1 < len(lines) and "PRE-FLOP" in lines[i] and lines[i + 1].strip().endswith(" folds"):
                        bu_folds_hand = True
                        i += 1
                    i += 1
                if bu_folds_hand and seats >= 3:
                    handler.writelines(lines[si:i])
                    handler.write("\n")
                    bu_folds_hands_copied += 1
                    bu_folds_hands_i += 1
                if bu_folds_hands_i == HANDS_IN_FILE:
                    handler.flush()
                    handler.close()
                    bu_folds_hands_i = 0
                    step += 1
                    file_name = path_data_file_name.format(str(step).zfill(zfill_len))
                    handler = open(file_name, "w", encoding="utf-8")
        handler.flush()
        handler.close()
        logging.info("Copied {} bu folds hands into files".format(bu_folds_hands_copied))


class FastStatisticCalculator(AbstractStatisticCalculator):

    def __init__(self, calcdata, calctsdata, colormarkers, result, original_interval, current_interval, nicknames):
        super().__init__(calcdata, calctsdata, colormarkers, result, original_interval, current_interval)
        self.self_nicknames = nicknames

    def calculate(self, calcmode, regtables, reghands, interval, buyin, last):
        logging.info("Run FAST stat calculation")
        files_to_calculate = self.__get_calc_files__(interval)
        colored_players = self.__get_colored_players__()
        table_stats, last_table_stats = self.__get_stats__(
            files_to_calculate, calcmode, colored_players, regtables, reghands, interval, buyin, last
        )
        header_lines = self.__get_stat_lines_header__(calcmode, regtables, reghands, interval, buyin)
        reduced_stat_lines, stat_lines = AbstractStatisticCalculator.__get_stat_lines__(
            table_stats, last_table_stats=last_table_stats
        )
        logging.info("Finish FAST stat calculation")

        return header_lines, reduced_stat_lines

    def __get_calc_files__(self, interval):
        logging.info("Start getting files to calculate stat")
        data_file_idx = defaultdict(set)
        count = 0

        file_root_filter = interval_file_filter(interval)
        for root, _, files in os.walk(self.path_calc_data):
            if not file_root_filter(root):
                continue
            for file in filter(is_data_file, files):
                table_id = parse_table_id(file)
                data_file_idx[table_id].add(os.path.join(root, file))
                count += 1
        logging.info("Data files to calculate stat: {} ".format(count))

        result = []
        for table_id, data_files in data_file_idx.items():
            data_files = data_file_idx[table_id]
            result.append(TableFileMeta(table_id, "", data_files))

        logging.info("Total file entities to calculate stat: {}".format(len(result)))

        return result

    def __get_stats__(self, files_to_calculate, calcmode, colored_players, regtables, reghands, interval, buyin, last):
        logging.info("Start getting table stats")

        table_stats = Counter()
        last_table_stats = Counter()
        filter_count = 0
        filter_out_count = 0
        not_found_count = 0
        filters = [interval_filter(interval)] + ([] if buyin is None else [buyin_filter(buyin)])
        regs = set()
        files_to_calculate.sort(key=lambda tfm: tfm.id, reverse=True)

        mod = statistic_mod(len(files_to_calculate))
        for i, file_to_calc in enumerate(files_to_calculate):
            if i % mod == 0:
                logging.info("Getting table stat from {}/{} files...".format(i, len(files_to_calculate)))
            table = self.index.get(file_to_calc.id, None)
            if table is None:
                not_found_count += 1
                table = self.__get_table_from_data_file__(file_to_calc)
            if not all(map(lambda f: f(table), filters)):
                filter_out_count += 1
                continue

            reg = 0
            fish = 0
            for nickname in map(lambda player: player.nickname, table.players):
                if nickname in regs:
                    reg += 1
                elif self.__is_reg__(nickname, colored_players, calcmode, regtables, reghands):
                    regs.add(nickname)
                    reg += 1
                else:
                    fish += 1
            key = (reg, fish)
            table_stats[key] += 1
            if i < last:
                last_table_stats[key] += 1
            filter_count += 1

        logging.info("Tables were not found in index to get data: {}".format(not_found_count))
        logging.info("Tables to calculate stat: {}".format(filter_count))
        logging.info("Tables were filtered out: {}".format(filter_out_count))
        logging.info("Got {} buckets in table stats: {}".format(len(table_stats), list(table_stats.keys())))

        return table_stats, last_table_stats

    def __get_table_from_data_file__(self, file_to_calc):
        data_file = list(sorted_data_files(file_to_calc.data_files))[0]
        with open(data_file, encoding="utf-8") as handler:
            lines = handler.readlines()
        i = 0
        while i < len(lines) and not bool(lines[i].strip()):
            i += 1
        timestamp, buy_in = parse_data_line(lines[i])
        while i < len(lines) and not lines[i].startswith("Seat"):
            i += 1
        players = set()
        while i < len(lines) and lines[i].startswith("Seat"):
            nickname = parse_nickname(lines[i])
            if nickname not in self.self_nicknames:
                players.add(nickname)
            i += 1

        table_data = TableData(timestamp, 0, buy_in)
        players = [Player(nickname, 0, 0) for nickname in players]
        return TableFast(file_to_calc.id, table_data, players)


def serialize_table(table):
    players = []
    for i, player in enumerate(table.players):
        players.append("{}:{}:{}".format(player.nickname, player.hands, player.seat))
    return "{}|{}|{}|{}|{}|{}|{}:{}:{}|{}".format(
        table.id,
        table.table_data.timestamp,
        table.table_data.prize_pool,
        table.table_data.buy_in,
        table.seat,
        ",".join(players),
        table.xa.nickname,
        table.xa.after_hand,
        table.xa.eliminated_by.value,
        table.lost_after_hand
    )


def deserialize_table(line):
    table_id, timestamp, prize_pool, buy_in, seat_str, players_str, xa_str, lost_after_hand = line.split("|")
    table_data = TableData(timestamp, float(prize_pool), float(buy_in))
    players = []
    for player in players_str.split(","):
        nickname, hands, seat = player.split(":")
        players.append(Player(nickname.strip(), int(hands.strip()), int(seat.strip())))
    xa_nickname, xa_after_hands, xa_eliminated_by = xa_str.split(":")
    xa = XA(xa_nickname.strip(), int(xa_after_hands.strip()), EliminatedType(xa_eliminated_by))

    return Table(int(table_id), table_data, int(seat_str), players, xa, int(lost_after_hand))


def serialize_indexed_player(index_player):
    return "{}|{}|{}".format(
        index_player.nickname,
        index_player.tables,
        index_player.hands
    )


def deserialized_indexed_player(line):
    nickname, tables, hands = line.split("|")
    return IndexedPlayer(nickname.strip(), int(tables), int(hands))


def sorted_data_files(data_files):
    sorted_tuples = sorted(tuple(map(parse_int, data_file.split(os.sep))) for data_file in data_files)
    if is_windows():
        return map(lambda tup: os.path.join(str(tup[0]), os.sep, *map(str, tup[1:])), sorted_tuples)
    else:
        return map(lambda tup: os.path.join(*map(str, tup)), sorted_tuples)


def get_folder_bucket_name(table_stat, xa_type):
    if xa_type == XAType.LOST:
        return str(table_stat.lost_after_hand)
    return os.path.join(table_stat.eliminated_by.value, str(table_stat.xa_after_hand))


def is_windows():
    return os.name == "nt"


def is_data_file(file):
    return file.startswith(EXPRESSO_NITRO)


def is_tsdata_file(file):
    return EXPRESSO_NITRO in file and LIMIT_SUMMARY in file


def is_color_marker_file(file):
    return file.endswith(WNX_CM)


def parse_table_id(file):
    return int(file.split("(")[1].split(")")[0])


def parse_buy_in(line):
    return sum(map(parse_price, line.split(":")[1].split("+")))


def parse_prize_pool(line):
    return parse_price(line.split(":")[1])


def parse_price(price):
    return float(price.strip()[:-1])


def parse_tournament_started(line):
    line_split = line.split()
    return "{} {}".format(line_split[2].strip(), line_split[3].strip())


def parse_data_line(line):
    line_split = line.split()
    timestamp = "{} {}".format(line_split[-3].strip(), line_split[-2].strip())
    i = line_split.index("buyIn:")
    buy_in = parse_price(line_split[i + 1]) + parse_price(line_split[i + 3])
    return timestamp, buy_in


def parse_seat_nickname(line):
    line_split = line.strip().split()
    return int(line_split[1][:-1]), " ".join(line_split[2:-1])


def parse_nickname(line):
    return " ".join(line.strip().split()[2:-1])


def parse_nickname_summary(line):
    line = line[:line.find("(")]
    return " ".join(line.strip().split()[2:])


def interval_filter(interval):
    return lambda table: interval[0] <= table.table_data.timestamp <= interval[1]


def interval_file_filter(interval):
    start = datetime.strptime(interval[0], DATETIME_FORMAT) - timedelta(days=2)
    end = datetime.strptime(interval[1], DATETIME_FORMAT) + timedelta(days=2)
    start, end = start.strftime(DATE_FORMAT), end.strftime(DATE_FORMAT)

    def internal(root_path):
        paths = root_path.split(os.sep)
        if len(paths) < 3:
            return False
        file_date = "{}/{}/{}".format(paths[-3], paths[-2].zfill(2), paths[-1].zfill(2))
        return start <= file_date <= end

    return internal


def buyin_filter(buyin):
    return lambda table: int(table.table_data.buy_in) == int(buyin)


def xa_filter(table_stat):
    return table_stat.xa_after_hand >= 0


def xa_lost_filter(table_stat):
    return table_stat.lost_after_hand > 0


def get_buy_in_mp(table, buyinsort):
    mp = int(table.table_data.prize_pool / table.table_data.buy_in) if table.table_data.buy_in > 0 else 0
    return "x{}".format(mp) if mp in buyinsort else "rest"


def is_reg_left(table, reg_player, key):
    return (table.seat % 3) + 1 == reg_player.seat if key == (1, 1) else None


def statistic_mod(last_number):
    return max(1, last_number // 10)


def parse_calcmode(mode):
    return CalcMode(mode.strip().lower())


def parse_nicknames(names):
    return set(map(lambda name: name.strip(), names.split(",")))


def parse_interval(interval):
    if "-" in interval:
        start, end = interval.split("-")
        start = "{}/{}/{} 00:00:00".format(start[0:4], start[4:6], start[6:])
        end = "{}/{}/{} 23:59:59".format(end[0:4], end[4:6], end[6:])
        start, end = datetime.strptime(start, DATETIME_FORMAT), datetime.strptime(end, DATETIME_FORMAT)
        start_utc, end_utc = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        start, end = start.strftime(DATETIME_FORMAT), end.strftime(DATETIME_FORMAT)
        start_utc, end_utc = start_utc.strftime(DATETIME_FORMAT), end_utc.strftime(DATETIME_FORMAT)

        return (start, end), (start_utc, end_utc)

    current = datetime.now()
    current_utc = current.astimezone(timezone.utc)
    end = current.strftime(DATETIME_FORMAT)
    end_utc = current_utc.strftime(DATETIME_FORMAT)

    if interval == "all":
        start = "0000/00/00 00:00:00"
        start_utc = "0000/00/00 00:00:00"
        return (start, end), (start_utc, end_utc)

    if interval == "today":
        start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == "month":
        start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif "h" in interval:
        start = current - timedelta(hours=int(interval[:-1]))
    else:
        raise ValueError("Cannot parse interval: {}".format(interval))
    start_utc = start.astimezone(timezone.utc)

    return (start.strftime(DATETIME_FORMAT), end), (start_utc.strftime(DATETIME_FORMAT), end_utc)


def parse_bool(value):
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    raise ValueError("Cannot parse bool value: {}".format(value))


def parse_int(s):
    try:
        return int(s)
    except ValueError:
        return s


def parse_buyin(buyin):
    if buyin == "all":
        return None
    return float(buyin)


def parse_buyin_sort(buyinsort):
    return set(map(lambda b: int(b.strip()[1:]), buyinsort.split(",")))


def parse_file_args(file):
    result = {}
    if os.path.exists(file):
        with open(file, encoding="utf8") as handler:
            config = handler.readlines()
        for k, v in map(lambda s: s.split("=", 1), filter(lambda s: s.strip(), config)):
            result[k.strip()] = v.strip()

    return result


def parse_args(config_file="config.txt"):
    parser = argparse.ArgumentParser(description="Reg and fish statistic")
    parser.add_argument("--recalc", metavar="bool", help="full with index or fast statistic")
    parser.add_argument("--result", metavar="path", help="path to result folder")
    parser.add_argument("--nicknames", metavar="name[,name]", help="player nicknames")
    parser.add_argument("--data", metavar="path", help="path to data folder")
    parser.add_argument("--tsdata", metavar="path", help="path to TS data folder")
    parser.add_argument("--calcdata", metavar="path", help="path to data folder on which calculate statistic")
    parser.add_argument("--calctsdata", metavar="path", help="path to TS data folder on which calculate statistic")
    parser.add_argument("--colormarkers", metavar="path", help="path to color markers folder")
    parser.add_argument("--calcmode", metavar="mode", help="calculation mode tables/hands")
    parser.add_argument("--regtables", metavar="int", help="table count for reg", type=int)
    parser.add_argument("--reghands", metavar="int", help="hand count for reg", type=int)
    parser.add_argument("--interval", metavar="interval", help="interval e.g. 1h,2d,today,month,all,20240101-20240201")
    parser.add_argument("--buyin", metavar="value", help="buyin to filter e.g. 5,10,all")
    parser.add_argument("--last", metavar="int", help="calculate stat for last tables additionally")
    parser.add_argument("--sort", metavar="bool", help="sort files into folders")
    parser.add_argument("--buyinsort", metavar="x2[,x3]", help="list of buyin multipliers to sort")
    parser.add_argument("--xa", metavar="bool", help="sort files by XA")
    parser.add_argument("--firsthand", metavar="bool", help="sort first hands into files")
    parser.add_argument("--bufolds", metavar="bool", help="sort button folds hands into files")
    parser.add_argument("--interactive", metavar="bool", help="interactive mode for FAST mode")
    args = parser.parse_args()
    config_args = parse_file_args(config_file)

    if not args.recalc:
        args.recalc = config_args.get("recalc", "false")
    args.recalc = parse_bool(args.recalc)
    if not args.result:
        args.result = config_args.get("result", "./")
    if not args.nicknames:
        args.nicknames = config_args.get("nicknames", "")
    args.nicknames = parse_nicknames(args.nicknames)
    if not args.data:
        if "data" not in config_args:
            raise ValueError("No path specified for data folder")
        else:
            args.data = config_args["data"]
    if not args.tsdata:
        if "tsdata" not in config_args:
            raise ValueError("No path specified for TS data folder")
        else:
            args.tsdata = config_args["tsdata"]
    if not args.calcdata:
        if "calcdata" not in config_args:
            raise ValueError("No path specified for calculation data folder")
        else:
            args.calcdata = config_args["calcdata"]
    if not args.calctsdata:
        if "calctsdata" not in config_args:
            raise ValueError("No path specified for calculation TS data folder")
        else:
            args.calctsdata = config_args["calctsdata"]
    if not args.colormarkers:
        if "colormarkers" not in config_args:
            raise ValueError("No path specified for color markers folder")
        else:
            args.colormarkers = config_args["colormarkers"]
    if not args.calcmode:
        args.calcmode = config_args.get("calcmode", "tables")
    args.calcmode = parse_calcmode(args.calcmode)
    if not args.regtables:
        args.regtables = int(config_args.get("regtables", "100"))
    if not args.reghands:
        args.reghands = int(config_args.get("reghands", "300"))
    if not args.interval:
        args.interval = config_args.get("interval", "all")
    args.current_interval, args.utc_interval = parse_interval(args.interval)
    if not args.buyin:
        args.buyin = config_args.get("buyin", "all")
    args.buyin = parse_buyin(args.buyin)
    if not args.last:
        args.last = config_args.get("last", "50")
    args.last = int(args.last)
    if not args.sort:
        args.sort = config_args.get("sort", "false")
    args.sort = parse_bool(args.sort)
    if not args.buyinsort:
        args.buyinsort = config_args.get("buyinsort", "x2")
    args.buyinsort = parse_buyin_sort(args.buyinsort)
    if not args.xa:
        args.xa = config_args.get("xa", "false")
    args.xa = parse_bool(args.xa)
    if not args.firsthand:
        args.firsthand = config_args.get("firsthand", "false")
    args.firsthand = parse_bool(args.firsthand)
    if not args.bufolds:
        args.bufolds = config_args.get("bufolds", "false")
    args.bufolds = parse_bool(args.bufolds)
    if not args.interactive:
        args.interactive = config_args.get("interactive", "false")
    args.interactive = parse_bool(args.interactive)

    return args


def index(data, tsdata, result, nicknames):
    index_creator = IndexCreator(data, tsdata, result, nicknames)
    index_creator.index()


def calculate_full(
        result, calcdata, calctsdata, colormarkers, calcmode, regtables, reghands,
        original_interval, current_interval, interval, buyin, is_sort, buyinsort, is_xa, is_first_hand, if_bu_fold
):
    calculator = FullStatisticCalculator(
        calcdata, calctsdata, colormarkers, result, original_interval, current_interval
    )
    stat_lines = calculator.calculate(
        calcmode, regtables, reghands, interval, buyin, is_sort, buyinsort, is_xa, is_first_hand, if_bu_fold
    )
    print_stat_lines(stat_lines)


def calculate_fast(
        result, calcdata, calctsdata, colormarkers, calcmode, regtables, reghands,
        original_interval, current_interval, interval, buyin, last, interactive, nicknames
):
    statistic_calculator = FastStatisticCalculator(
        calcdata, calctsdata, colormarkers, result, original_interval, current_interval, nicknames
    )
    header_lines, stat_lines = statistic_calculator.calculate(calcmode, regtables, reghands, interval, buyin, last)
    print_result_lines(header_lines, stat_lines)
    while interactive:
        logging.info("")
        logging.info("")
        logging.info("Type command")
        inp = input()
        if inp.strip().lower() == "exit":
            logging.info("Closing program...")
            break
        header_lines, stat_lines = statistic_calculator.calculate(calcmode, regtables, reghands, interval, buyin, last)
        print_result_lines(header_lines, stat_lines)


def print_result_lines(header_lines, stat_lines):
    logging.info("")
    logging.info("")
    logging.info("==================================================")
    for line in header_lines:
        logging.info(line)
    print_stat_lines(stat_lines, sep=False)
    logging.info("==================================================")


def print_stat_lines(stat_lines, sep=True):
    if sep:
        logging.info("")
        logging.info("")
        logging.info("==================================================")
    logging.info("Result statistic:")
    for line in stat_lines:
        logging.info("  {}".format(line))
    if sep:
        logging.info("==================================================")


def main():
    args = parse_args()
    logging.info("Recalc:                        {}".format(args.recalc))
    logging.info("Result folder:                 {}".format(args.result))
    logging.info("Nicknames:                     {}".format(args.nicknames))
    logging.info("Data folder:                   {}".format(args.data))
    logging.info("TS data folder:                {}".format(args.tsdata))
    logging.info("Calculation data folder:       {}".format(args.calcdata))
    logging.info("Calculation TS data folder:    {}".format(args.calctsdata))
    logging.info("Color Markers folder:          {}".format(args.colormarkers))
    logging.info("Calculation mode:              {}".format(args.calcmode.name))
    logging.info("Reg tables:                    {}".format(args.regtables))
    logging.info("Reg hands:                     {}".format(args.reghands))
    logging.info("Original interval:             {}".format(args.interval))
    logging.info("Interval:                      {}".format(args.current_interval))
    logging.info("Interval UTC:                  {}".format(args.utc_interval))
    logging.info("Buy-in:                        {}".format(args.buyin))
    logging.info("Last tables:                   {}".format(args.last))
    logging.info("Sort files into folders:       {}".format(args.sort))
    logging.info("Buy-in sort by:                {}".format(args.buyinsort))
    logging.info("Sort files by XA:              {}".format(args.xa))
    logging.info("Sort hands by first:           {}".format(args.firsthand))
    logging.info("Sort hands by button folds:    {}".format(args.bufolds))
    logging.info("Interactive mode:              {}".format(args.interactive))
    logging.info("")

    if args.recalc:
        # index(args.data, args.tsdata, args.result, args.nicknames)
        calculate_full(
            args.result,
            args.calcdata,
            args.calctsdata,
            args.colormarkers,
            args.calcmode,
            args.regtables,
            args.reghands,
            args.interval,
            args.current_interval,
            args.utc_interval,
            args.buyin,
            args.sort,
            args.buyinsort,
            args.xa,
            args.firsthand,
            args.bufolds
        )
    else:
        calculate_fast(
            args.result,
            args.calcdata,
            args.calctsdata,
            args.colormarkers,
            args.calcmode,
            args.regtables,
            args.reghands,
            args.interval,
            args.current_interval,
            args.utc_interval,
            args.buyin,
            args.last,
            args.interactive,
            args.nicknames
        )

    return 0


if __name__ == "__main__":
    main()
