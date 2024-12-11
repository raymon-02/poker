import argparse
import json
import logging
import os
from collections import defaultdict, Counter, OrderedDict
from datetime import timezone, datetime, timedelta
from shutil import copyfile
from types import SimpleNamespace
from typing import NamedTuple

FORMAT = "%(asctime)-15s [%(levelname)8s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

ID_FILE_NAME = "id.txt"
INDEX_FILE_NAME = "index.txt"
PLAYER_FILE_NAME = "players.txt"
STAT_FILE_NAME = "stats.txt"

EXPRESSO_NITRO = "Expresso Nitro"
LIMIT_SUMMARY = "limit_summary"
WNX_CM = "WNX.cm"
DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"

WEEKDAY = "Weekday"
WEEKEND = "Weekend"
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", WEEKDAY, "Saturday", "Sunday", WEEKEND]
WEEKENDS = {"Saturday", "Sunday"}


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


class XA(NamedTuple):
    nickname: str
    after_hand: int


NO_XA = XA("", -1)


class IndexedPlayer(NamedTuple):
    nickname: str
    tables: int
    hands: int
    color_status: str


class Table(NamedTuple):
    id: int
    table_data: TableData
    players: list[Player]
    xa: XA


class TableStat(NamedTuple):
    file_meta: TableFileMeta
    is_x2: bool
    xa_after_hand: int


class Report(NamedTuple):
    days: OrderedDict[str, Counter[tuple[int, int]]]
    hours: OrderedDict[int, Counter[tuple[int, int]]]
    threes: OrderedDict[int, Counter[tuple[int, int]]]


class IndexCreator:

    def __init__(self, data, tsdata, colormarkers, result, nicknames):
        self.path_data = data
        self.path_tsdata = tsdata
        self.path_colormarkers = colormarkers
        self.path_result = result
        self.self_nicknames = nicknames
        self.id_file = os.path.join(result, ID_FILE_NAME)
        self.index_file = os.path.join(result, INDEX_FILE_NAME)
        self.player_file = os.path.join(result, PLAYER_FILE_NAME)
        self.__index_files_init__()

    def __index_files_init__(self):
        if os.path.exists(self.id_file) and os.path.exists(self.index_file):
            return
        with open(self.id_file, "w", encoding="utf-8") as handler:
            handler.write("0")
        with open(self.index_file, "w", encoding="utf-8") as handler:
            handler.write("")
        with open(self.player_file, "w", encoding="utf-8") as handler:
            handler.write("")

    def modify_index(self, recalc):
        logging.info("Run index modification")
        if not recalc:
            logging.info("Recalculation of index on each run is turned off. Skipping index modification")
            return

        last_table_id = self.__get_last_table_id__()
        files_to_index = self.__get_files_to_index__(last_table_id)
        colored_players = self.__get_colored_players__()
        indexed_players = self.__get_indexed_players__()

        new_last_table_id = last_table_id
        tables = []
        if files_to_index:
            new_last_table_id = max(map(lambda fmd: fmd.id, files_to_index))
            tables = self.__get_tables__(files_to_index)
        logging.info("New last table id: {}".format(new_last_table_id))
        new_indexed_players = IndexCreator.__get_new_indexed_players__(colored_players, indexed_players, tables)
        self.__write_index__(new_last_table_id, tables, new_indexed_players)

        logging.info("Finish index modification")

    def __get_last_table_id__(self):
        with open(self.id_file, encoding="utf-8") as handler:
            last_table_id = int(handler.readline().strip())
        logging.info("Last table id: {}".format(last_table_id))
        return last_table_id

    def __get_files_to_index__(self, last_table_id):
        logging.info("Start getting files to index")
        data_file_idx = defaultdict(set)
        tsdata_file_idx = {}
        count = 0
        for root, _, files in os.walk(self.path_data):
            for file in filter(is_data_file, files):
                table_id = parse_table_id(file)
                if table_id > last_table_id:
                    data_file_idx[table_id].add(os.path.join(root, file))
                    count += 1
        logging.info("Data files to index: {} ".format(count))

        count = 0
        for root, _, files in os.walk(self.path_tsdata):
            for file in filter(is_tsdata_file, files):
                table_id = parse_table_id(file)
                if table_id > last_table_id:
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

    def __get_colored_players__(self):
        logging.info("Start getting colored players")
        colored_players = {}
        for root, _, files in os.walk(self.path_colormarkers):
            for file in filter(is_color_marker_file, files):
                filename = os.path.join(root, file)
                with open(filename, encoding="utf-8") as handler:
                    text = "".join(handler.readlines())
                    player = json.loads(text, object_hook=lambda d: SimpleNamespace(**d))
                    colored_players[player.Player.Nickname] = "Reg" if player.ColorMarker.IsReg else "Fish"

        logging.info("Got {} colored players".format(len(colored_players)))

        return colored_players

    def __get_indexed_players__(self):
        logging.info("Start getting current indexed players")
        indexed_players = {}
        with open(self.player_file, encoding="utf-8") as handler:
            lines = handler.readlines()
        for line in lines:
            indexed_player = deserialized_indexed_player(line)
            indexed_players[indexed_player.nickname] = indexed_player

        logging.info("Got {} current indexed players".format(len(indexed_players)))

        return indexed_players

    def __get_tables__(self, files_to_index):
        logging.info("Start getting tables from {} files".format(len(files_to_index)))
        tables = []
        mod = statistic_mod(len(files_to_index))
        for i, file_to_index in enumerate(files_to_index):
            if i % mod == 0:
                logging.info("Getting table from {}/{} files...".format(i, len(files_to_index)))
            table_data = IndexCreator.__get_table_data__(file_to_index.tsdata_file)
            if not table_data:
                logging.warning("Cannot get table data from TS file. Skipping: {}".format(file_to_index.tsdata_file))
                continue
            players, xa = self.__get_players__(file_to_index.data_files)
            tables.append(Table(file_to_index.id, table_data, players, xa))

        logging.info("Got {} tables from files".format(len(tables)))

        return tables

    @staticmethod
    def __get_new_indexed_players__(colored_players, indexed_players, tables):
        logging.info("Start calculating indexed players")
        new_indexed_players = {}

        player_tables = Counter()
        player_hands = Counter()
        for nickname, index_player in indexed_players.items():
            player_tables[nickname] += index_player.tables
            player_hands[nickname] += index_player.hands
        for table in tables:
            for player in table.players:
                player_tables[player.nickname] += 1
                player_hands[player.nickname] += player.hands

        for nickname, color_status in colored_players.items():
            indexed_player = IndexedPlayer(nickname, player_tables[nickname], player_hands[nickname], color_status)
            new_indexed_players[nickname] = indexed_player
        for nickname, table_count in player_tables.items():
            if nickname not in new_indexed_players:
                indexed_player = IndexedPlayer(nickname, table_count, player_hands[nickname], "None")
                new_indexed_players[nickname] = indexed_player

        logging.info("Calculated {} indexed players".format(len(new_indexed_players)))

        return list(new_indexed_players.values())

    @staticmethod
    def __get_table_data__(tsdata_file):
        with open(tsdata_file, encoding="utf-8") as handler:
            lines = handler.readlines()
        buy_in, prize_pool, timestamp = None, None, None
        for line in lines:
            if line.startswith("Buy-In"):
                buy_in = parse_buy_in(line)
            elif line.startswith("Prizepool"):
                prize_pool = parse_prize_pool(line)
            elif line.startswith("Tournament started"):
                timestamp = parse_tournament_started(line)

        if buy_in is None or prize_pool is None or timestamp is None:
            return None

        return TableData(timestamp, prize_pool, buy_in)

    def __get_players__(self, data_files):
        xa = None
        counter = Counter()
        lines = []
        for data_file in sorted_data_files(data_files):
            with open(data_file, encoding="utf-8") as handler:
                lines.extend(handler.readlines())

        i = 0
        hand_count = 0
        while i < len(lines):
            while i < len(lines) and not bool(lines[i].strip()):
                i += 1
            while i < len(lines) and not lines[i].startswith("Seat"):
                i += 1
            xa_nicknames = set()
            while i < len(lines) and lines[i].startswith("Seat"):
                nickname = parse_nickname(lines[i])
                if nickname not in self.self_nicknames:
                    counter[nickname] += 1
                xa_nicknames.add(nickname)
                i += 1
            xa_diff = xa_nicknames.difference(self.self_nicknames)
            if xa is None and len(xa_nicknames) == 2 and len(xa_diff) == 1:
                nickname = next(iter(xa_diff))
                xa = XA(nickname, hand_count)
            while i < len(lines) and bool(lines[i].strip()):
                i += 1
            hand_count += 1

        players = [Player(nickname, count) for nickname, count in counter.items()]

        return players, xa if xa else NO_XA

    def __write_index__(self, new_last_table_id, tables, new_indexed_players):
        logging.info("Adding {} tables into index file...".format(len(tables)))
        serialized_tables = map(serialize_table, tables)
        mod = statistic_mod(len(tables))
        with open(self.index_file, "a+", encoding="utf-8") as handler:
            for i, serialized_table in enumerate(serialized_tables):
                if i % mod == 0:
                    logging.info("Adding {}/{} table into index file...".format(i, len(tables)))
                handler.write(serialized_table)
                handler.write("\n")
        logging.info("Added tables into index file")

        logging.info("Saving {} indexed players...".format(len(new_indexed_players)))
        with open(self.player_file, "w", encoding="utf-8") as handler:
            for indexed_player in new_indexed_players:
                handler.write(serialize_indexed_player(indexed_player))
                handler.write("\n")
        logging.info("Saved indexed players")

        logging.info("Saving new last table id: {}...".format(new_last_table_id))
        with open(self.id_file, "w", encoding="utf-8") as handler:
            handler.write(str(new_last_table_id))
        logging.info("Saved new last table id")


class StatisticCalculator:

    def __init__(self, calcdata, calctsdata, result, current_interval):
        self.path_calc_data = calcdata
        self.path_calc_tsdata = calctsdata
        self.path_result = result
        self.current_interval = current_interval
        self.index_file = os.path.join(result, INDEX_FILE_NAME)
        self.player_file = os.path.join(result, PLAYER_FILE_NAME)
        self.index = self.__get_index__()
        self.players = self.__get_players__()
        self.files_to_calculate = self.__get_calc_files__()

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

    def calculate(self, calcmode, regtables, reghands, interval, buyin, is_report, is_sort, is_xa):
        logging.info("Run stat calculation")
        table_stats, report = self.__get_stats__(calcmode, regtables, reghands, interval, buyin, is_report)
        header_lines = self.__get_stat_lines_header__(calcmode, regtables, reghands, interval, buyin)
        stat_lines = StatisticCalculator.__get_stat_lines__(table_stats, report, is_report)
        path_result_run = self.__generate_result_run_folder__()
        StatisticCalculator.__write_stats__(header_lines, stat_lines, path_result_run)
        if is_sort:
            StatisticCalculator.__copy_data_files__(table_stats, path_result_run)
        if is_xa:
            StatisticCalculator.__copy_xa_data_files__(table_stats, path_result_run)
        logging.info("Finish stat calculation")

        return stat_lines

    def __get_stats__(self, calcmode, regtables, reghands, interval, buyin, is_report):
        logging.info("Start getting table stats")

        table_stats = defaultdict(list)
        filter_count = 0
        filter_out_count = 0
        not_found_count = 0
        filters = [interval_filter(interval)] + ([] if buyin is None else [buyin_filter(buyin)])
        report = StatisticCalculator.__generate_report_keys__()

        for file_to_calc in self.files_to_calculate:
            table = self.index.get(file_to_calc.id, None)
            if table is None:
                not_found_count += 1
                continue
            if not all(map(lambda f: f(table), filters)):
                filter_out_count += 1
                continue

            reg = 0
            fish = 0
            for nickname in map(lambda player: player.nickname, table.players):
                if self.__is_reg__(nickname, calcmode, regtables, reghands):
                    reg += 1
                else:
                    fish += 1
            key = (reg, fish)
            xa_after_hand = (
                -1 if self.__is_reg__(table.xa.nickname, calcmode, regtables, reghands) else table.xa.after_hand
            )
            table_stats[key].append(
                TableStat(file_to_calc, is_prize_pool_x2(table), xa_after_hand)
            )
            if is_report:
                StatisticCalculator.__add_to_report__(report, table, key)
            filter_count += 1

        logging.info("Tables to calculate stat: {}".format(filter_count))
        logging.info("Tables were filtered out: {}".format(filter_out_count))
        if not_found_count > 0:
            logging.info("Tables were not found: {}. Needs to rerun index with calc files".format(not_found_count))
        logging.info("Got {} buckets in table stats: {}".format(len(table_stats), list(table_stats.keys())))

        return table_stats, report

    def __is_reg__(self, nickname, calcmode, regtables, reghands):
        player = self.players.get(nickname, None)
        if player is None or player.color_status == "Fish":
            return False
        if player.color_status == "Reg":
            return True
        if calcmode == "tables":
            return player.tables >= regtables
        else:
            return player.hands >= reghands

    @staticmethod
    def __generate_report_keys__():
        days = OrderedDict()
        for day_name in DAY_NAMES:
            days[day_name] = Counter()

        hours = OrderedDict()
        for hour in range(24):
            hours[hour] = Counter()

        threes = OrderedDict()
        for hour in range(0, 24, 3):
            threes[hour] = Counter()

        return Report(days, hours, threes)

    @staticmethod
    def __add_to_report__(report, table, key):
        timestamp_utc = datetime.strptime(table.table_data.timestamp, DATETIME_FORMAT).replace(tzinfo=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        table_timestamp = timestamp_utc.astimezone(local_tz)
        day_name = table_timestamp.strftime('%A')
        report.days[day_name][key] += 1
        if day_name in WEEKENDS:
            report.days[WEEKEND][key] += 1
        else:
            report.days[WEEKDAY][key] += 1
        report.hours[table_timestamp.hour][key] += 1
        report.threes[table_timestamp.hour // 3 * 3][key] += 1

    def __get_stat_lines_header__(self, calcmode, regtables, reghands, interval, buyin):
        header_lines = []
        current_time = datetime.now().strftime(DATETIME_FORMAT)
        header_lines.append("Run time:             {}".format(current_time))
        header_lines.append("Reg calculation mode: {}".format(calcmode))
        header_lines.append("Reg table value:      {}".format(regtables))
        header_lines.append("Reg hand value:       {}".format(reghands))
        header_lines.append("In interval:          {}".format(self.current_interval))
        header_lines.append("In interval UTC:      {}".format(interval))
        header_lines.append("Buy-in:               {}".format(buyin if buyin else "all"))
        header_lines.append("")
        header_lines.append("")

        return header_lines

    @staticmethod
    def __get_stat_lines__(table_stats, report, is_report):
        stat_lines = []
        keys = sorted(table_stats.keys())
        StatisticCalculator.__get_stat_counter_lines__(table_stats, "tables", keys, stat_lines, map_f=len)

        if not is_report:
            return stat_lines

        stat_lines.append("")
        stat_lines.append("BY DAYS")
        for day_name, counter in report.days.items():
            StatisticCalculator.__get_stat_counter_lines__(counter, day_name, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY HOURS")
        for hour, counter in report.hours.items():
            hour_range = "{}-{}".format(hour, hour + 1)
            StatisticCalculator.__get_stat_counter_lines__(counter, hour_range, keys, stat_lines)

        stat_lines.append("")
        stat_lines.append("BY THREE HOURS")
        for hour, counter in report.threes.items():
            hour_range = "{}-{}".format(hour, hour + 3)
            StatisticCalculator.__get_stat_counter_lines__(counter, hour_range, keys, stat_lines)

        return stat_lines

    @staticmethod
    def __get_stat_counter_lines__(counter, total_name, keys, lines, map_f=lambda el: el):
        total = sum(map(map_f, counter.values()))
        lines.append("Total {}: {}".format(total_name, total))
        for key in keys:
            count = map_f(counter[key])
            reg, fish = key
            per = count / total * 100 if total > 0 else 0
            lines.append("Reg: {}, Fish {}, count: {:7}, rate: {:0.3f}%".format(reg, fish, count, per))
        lines.append("")

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
            folder_name = "reg-{}-fish-{}".format(reg, fish)
            path_data_x2 = os.path.join(path_result_run, folder_name, "x2", "data")
            path_tsdata_x2 = os.path.join(path_result_run, folder_name, "x2", "tsdata")
            path_data_rest = os.path.join(path_result_run, folder_name, "rest", "data")
            path_tsdata_rest = os.path.join(path_result_run, folder_name, "rest", "tsdata")
            os.makedirs(path_data_x2, exist_ok=True)
            os.makedirs(path_tsdata_x2, exist_ok=True)
            os.makedirs(path_data_rest, exist_ok=True)
            os.makedirs(path_tsdata_rest, exist_ok=True)
            mod = statistic_mod(len(table_stat_buket))
            for i, table_stat in enumerate(table_stat_buket):
                if i % mod == 0:
                    logging.info("Copying data files fox x2 and rest {}/{}...".format(i, len(table_stat_buket)))
                path_data, path_tsdata = (
                    (path_data_x2, path_tsdata_x2) if table_stat.is_x2 else (path_data_rest, path_tsdata_rest)
                )
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
        xa_filter_out_count = 0
        for (reg, fish), table_stat_buket in table_stats.items():
            logging.info("Copying data files for XA: reg={}, fish={}...".format(reg, fish))
            folder_name = "reg-{}-fish-{}".format(reg, fish)

            xa_folder_name = os.path.join(path_result_run, folder_name, "xa")
            table_stats_with_xa = list(filter(xa_filter, table_stat_buket))
            xa_filter_out_count += len(table_stat_buket) - len(table_stats_with_xa)
            mod = statistic_mod(len(table_stats_with_xa))
            for i, table_stat in enumerate(table_stats_with_xa):
                path_data = os.path.join(xa_folder_name, str(table_stat.xa_after_hand), "data")
                path_tsdata = os.path.join(xa_folder_name, str(table_stat.xa_after_hand), "tsdata")
                os.makedirs(path_data, exist_ok=True)
                os.makedirs(path_tsdata, exist_ok=True)
                if i % mod == 0:
                    logging.info("Copying data files for XA {}/{}...".format(i, len(table_stats_with_xa)))
                copy_tsdata_file = os.path.join(path_tsdata, table_stat.file_meta.tsdata_file.split(os.sep)[-1])
                copyfile(table_stat.file_meta.tsdata_file, copy_tsdata_file)
                for data_file in table_stat.file_meta.data_files:
                    copy_data_file = os.path.join(path_data, data_file.split(os.sep)[-1])
                    copyfile(data_file, copy_data_file)

            logging.info("Copied data files for XA: reg={}, fish={}".format(reg, fish))

        logging.info("File entries for XA were filtered out: {}".format(xa_filter_out_count))
        logging.info("Copied data files for XA")


def serialize_table(table):
    players = []
    for i, player in enumerate(table.players):
        players.append("{}:{}".format(player.nickname, player.hands))
    return "{}|{}|{}|{}|{}|{}:{}".format(
        table.id,
        table.table_data.timestamp,
        table.table_data.prize_pool,
        table.table_data.buy_in,
        ",".join(players),
        table.xa.nickname,
        table.xa.after_hand
    )


def deserialize_table(line):
    table_id, timestamp, prize_pool, buy_in, players_str, xa_str = line.split("|")
    table_data = TableData(timestamp, float(prize_pool), float(buy_in))
    players = []
    for player in players_str.split(","):
        nickname, hands = player.split(":")
        players.append(Player(nickname.strip(), int(hands.strip())))
    xa_nickname, xa_after_hands = xa_str.split(":")
    xa = XA(xa_nickname.strip(), int(xa_after_hands.strip()))

    return Table(int(table_id), table_data, players, xa)


def serialize_indexed_player(index_player):
    return "{}|{}|{}|{}".format(
        index_player.nickname,
        index_player.tables,
        index_player.hands,
        index_player.color_status
    )


def deserialized_indexed_player(line):
    nickname, tables, hands, color_status = line.split("|")
    return IndexedPlayer(nickname.strip(), int(tables), int(hands), color_status.strip())


def sorted_data_files(data_files):
    sorted_tuples = sorted(tuple(map(parse_int, data_file.split(os.sep))) for data_file in data_files)
    if is_windows():
        return map(lambda tup: os.path.join(str(tup[0]), os.sep, *map(str, tup[1:])), sorted_tuples)
    else:
        return map(lambda tup: os.path.join(*map(str, tup)), sorted_tuples)


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


def parse_nickname(line):
    return " ".join(line.strip().split()[2:-1])


def interval_filter(interval):
    return lambda table: interval[0] <= table.table_data.timestamp <= interval[1]


def buyin_filter(buyin):
    return lambda table: int(table.table_data.buy_in) == int(buyin)


def xa_filter(table_stat):
    return table_stat.xa_after_hand > 0


def is_prize_pool_x2(table):
    return 2 * int(table.table_data.buy_in) == int(table.table_data.prize_pool)


def statistic_mod(last_number):
    return max(1, last_number // 10)


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


def parse_file_args(file):
    result = {}
    if os.path.exists(file):
        with open(file, encoding="utf8") as handler:
            config = handler.readlines()
        for k, v in map(lambda s: s.split("=", 1), filter(lambda s: s.strip(), config)):
            result[k.strip()] = v.strip()

    return result


def parse_args(config_file="config.txt"):
    parser = argparse.ArgumentParser(description="Retrieve hands")
    parser.add_argument("-d", "--data", metavar="path", help="path to data folder")
    parser.add_argument("-t", "--tsdata", metavar="path", help="path to TS data folder")
    parser.add_argument("-c", "--colormarkers", metavar="path", help="path to color markers folder")
    parser.add_argument("-r", "--result", metavar="path", help="path to result folder")
    parser.add_argument("-n", "--nicknames", metavar="name[,name]", help="player nicknames")
    parser.add_argument("--recalc", metavar="bool", help="calculate index on each run")
    parser.add_argument("--calcdata", metavar="path", help="path to data folder on which calculate statistic")
    parser.add_argument("--calctsdata", metavar="path", help="path to TS data folder on which calculate statistic")
    parser.add_argument("--calcmode", metavar="mode", help="calculation mode tables/hands")
    parser.add_argument("--regtables", metavar="int", help="table count for reg", type=int)
    parser.add_argument("--reghands", metavar="int", help="hand count for reg", type=int)
    parser.add_argument("--interval", metavar="interval", help="interval e.g. 1h,2d,today,month,all,20240101-20240201")
    parser.add_argument("--buyin", metavar="value", help="buyin to filter e.g. 5,10,all")
    parser.add_argument("--report", metavar="bool", help="report statistic by days and hours")
    parser.add_argument("--sort", metavar="bool", help="sort files into folders")
    parser.add_argument("--xa", metavar="bool", help="sort files by XA")
    args = parser.parse_args()
    config_args = parse_file_args(config_file)

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
    if not args.colormarkers:
        if "colormarkers" not in config_args:
            raise ValueError("No path specified for color markers folder")
        else:
            args.colormarkers = config_args["colormarkers"]
    if not args.result:
        args.result = config_args.get("result", "./")
    if not args.nicknames:
        args.nicknames = config_args.get("nicknames", "")
    args.nicknames = parse_nicknames(args.nicknames)
    if not args.recalc:
        args.recalc = config_args.get("recalc", "true")
    args.recalc = parse_bool(args.recalc)
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
    if not args.calcmode:
        args.calcmode = config_args.get("calcmode", "tables")
    if not args.regtables:
        args.regtables = int(config_args.get("regtables", "100"))
    if not args.reghands:
        args.reghands = int(config_args.get("reghands", "200"))
    if not args.interval:
        args.interval = config_args.get("interval", "all")
    args.current_interval, args.interval = parse_interval(args.interval)
    if not args.buyin:
        args.buyin = config_args.get("buyin", "all")
    args.buyin = parse_buyin(args.buyin)
    if not args.report:
        args.report = config_args.get("report", "true")
    args.report = parse_bool(args.report)
    if not args.sort:
        args.sort = config_args.get("sort", "false")
    args.sort = parse_bool(args.sort)
    if not args.xa:
        args.xa = config_args.get("xa", "false")
    args.xa = parse_bool(args.xa)

    return args


def index(data, tsdata, colormarkers, result, nicknames, recalc):
    index_creator = IndexCreator(data, tsdata, colormarkers, result, nicknames)
    index_creator.modify_index(recalc)


def calculate(
        calcdata, calctsdata, result, calcmode, regtables, reghands,
        current_interval, interval, buyin, is_report, is_sort, is_xa
):
    statistic_calculator = StatisticCalculator(calcdata, calctsdata, result, current_interval)
    stat_lines = statistic_calculator.calculate(
        calcmode, regtables, reghands, interval, buyin, is_report, is_sort, is_xa
    )

    logging.info("")
    logging.info("")
    logging.info("==================================================")
    logging.info("Result statistic:")
    for line in stat_lines:
        logging.info("  {}".format(line))
    logging.info("==================================================")


def main():
    args = parse_args()
    logging.info("Data folder:                   {}".format(args.data))
    logging.info("TS data folder:                {}".format(args.tsdata))
    logging.info("Color Markers folder:          {}".format(args.colormarkers))
    logging.info("Result folder:                 {}".format(args.result))
    logging.info("Nicknames:                     {}".format(args.nicknames))
    logging.info("Recalculate index on each run: {}".format(args.recalc))
    logging.info("Calculation data folder:       {}".format(args.calcdata))
    logging.info("Calculation TS data folder:    {}".format(args.calctsdata))
    logging.info("Calculation mode:              {}".format(args.calcmode))
    logging.info("Reg tables:                    {}".format(args.regtables))
    logging.info("Reg hands:                     {}".format(args.reghands))
    logging.info("Interval:                      {}".format(args.current_interval))
    logging.info("Interval UTC:                  {}".format(args.interval))
    logging.info("Buy-in:                        {}".format(args.buyin))
    logging.info("Calculate report by intervals: {}".format(args.report))
    logging.info("Sort files into folders:       {}".format(args.sort))
    logging.info("Sort files by XA:              {}".format(args.xa))
    logging.info("")

    index(args.data, args.tsdata, args.colormarkers, args.result, args.nicknames, args.recalc)
    calculate(
        args.calcdata,
        args.calctsdata,
        args.result,
        args.calcmode,
        args.regtables,
        args.reghands,
        args.current_interval,
        args.interval,
        args.buyin,
        args.report,
        args.sort,
        args.xa
    )

    return 0


if __name__ == "__main__":
    main()
