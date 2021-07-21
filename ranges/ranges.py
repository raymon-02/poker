import argparse
import logging
import os
from collections import namedtuple, defaultdict
from datetime import datetime
from shutil import copyfile
from statistics import median, mean

FORMAT = "%(asctime)-15s [%(levelname)8s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

Range = namedtuple("Range", ["max", "name"])
File = namedtuple("File", ["name", "id", "path"])
Bounty = namedtuple("Bounty", ["count", "value"])
Stack = namedtuple("Stack", ["value", "chips"])
Score = namedtuple("Score", ["value", "stack", "eliminates", "win"])
Eliminate = namedtuple("Eliminate", ["multipliers", "values", "possibles"])
HandStat = namedtuple("HandStat", ["eliminate", "win"])
MAX_SCORE = 10000
INFO_FILE = "info_file"


class FileStructure:
    def __init__(self, path, mode):
        self.game_files = []
        self.stat_files = []
        self.__collect_files__(path)
        self.__apply_mode__(mode)

    def split(self, root, ranges, players, stack):
        logging.info("Start spitting...")
        range_map = self.__create_result_folder__(root, ranges)
        range_count = defaultdict(int)

        range_stack = defaultdict(list)
        range_eliminates = defaultdict(list)
        range_wins = defaultdict(list)

        stats = {}
        for sf in self.stat_files:
            if sf in stats:
                logging.warning("Two statistic files with id={}".format(sf.id))
            stats[sf.id] = sf

        gfc = 0
        sfc = 0
        for gf in self.game_files:
            score = self.__parse_file__(gf, players, stack)
            if score.value == -2:
                logging.warning("Cannot get bounties from file={}, full path={}".format(gf.name, gf.path))
                continue
            if score.value < 0:
                continue
            if score.value > MAX_SCORE:
                logging.warning("Score={} is too high file={}, full path={}".format(score.value, gf.name, gf.path))
                continue
            i = 0
            while score.value > ranges[i].max:
                i += 1
            copyfile(gf.path, os.path.join(range_map[ranges[i].max], gf.name))
            gfc += 1
            range_count[ranges[i].max] += 1
            if score.stack.value >= 0:
                range_stack[ranges[i].max].append(score.stack)
                range_eliminates[ranges[i].max].extend(score.eliminates)
                range_wins[ranges[i].max].append(score.win)
            else:
                logging.warning("Cannot get stack from file={}, full path={}".format(gf.name, gf.path))

            if gf.id in stats:
                copyfile(stats[gf.id].path, os.path.join(range_map[ranges[i].max], stats[gf.id].name))
                sfc += 1

        info_lines = []
        for r in ranges:
            info_lines.append("Range {:5}\n".format(r.max))

            stacks = range_stack[r.max]
            if stacks:
                stack_values = list(map(lambda s: s.value, stacks))
                stack_chips = list(map(lambda s: s.chips, stacks))
                info_lines.append("  Stack avg: {:0.3f}\n".format(mean(stack_values)))
                info_lines.append("  Stack med: {:0.3f}\n".format(median(stack_values)))
                info_lines.append("  Stack chips avg: {:0.3f}\n".format(mean(stack_chips)))
                info_lines.append("  Stack chips med: {:0.3f}\n".format(median(stack_chips)))

            rel = range_eliminates[r.max]
            wins = range_wins[r.max]
            if rel:
                eliminates = defaultdict(list)
                for el in rel:
                    for m in el.multipliers:
                        eliminates[m].append(el)
                eliminate_keys = sorted(eliminates.keys())
                info_lines.append("  Eliminates:\n")
                for el_key in eliminate_keys:
                    els = eliminates[el_key]
                    info_lines.append(
                        "    {}: {:0.2f}%   ({}/{})\n".format(el_key / 10, len(els) / len(rel) * 100, len(els),
                                                              len(rel))
                    )
                rel_values = []
                rel_possibles = []
                for el in rel:
                    rel_values.extend(el.values)
                    rel_possibles.extend(el.possibles)
                win_count = len(list(filter(lambda w: w, wins)))
                srv = sum(rel_values)
                srp = sum(rel_possibles)
                info_lines.append(
                    "  Eliminates avg: {:0.3f}   ({}/{})\n".format(len(rel) / len(wins) * 100, len(rel), len(wins))
                )
                info_lines.append("  Lost bounties: {:0.2f}%   ({:0.2f}/{:0.2f})\n".format(srv / srp * 100, srv, srp))
                info_lines.append("  Win bounty avg: {:0.3f}\n".format(mean(rel_values)))
                info_lines.append("  Win bounty med: {:0.3f}\n".format(median(rel_values)))
                info_lines.append(
                    "  Win rate: {:0.2f}%   ({}/{})\n".format(win_count / len(wins) * 100, win_count, len(wins))
                )
            info_lines.append("\n\n")
        with open(range_map[INFO_FILE], "w") as handler:
            handler.writelines(info_lines)

        logging.info("Done splitting")
        logging.info("")
        logging.info("Game files found   = {}".format(len(self.game_files)))
        logging.info("Game files handled = {}".format(gfc))
        logging.info("Game files error   = {}".format(len(self.game_files) - gfc))
        for r in ranges:
            logging.info("  Range {:5} = {}".format(r.max, range_count[r.max]))
        logging.info("Stat files found   = {}".format(len(self.stat_files)))
        logging.info("Stat files handled = {}".format(sfc))
        logging.info("No stat files      = {}".format(gfc - sfc))

    def __parse_file__(self, gf, players, stack):
        seats = self.__get_seats__(gf)
        if not seats:
            return Score(-1, -1, [], False)
        bounties = list(map(self.__get_bounty__, seats))
        bounties_avg = mean(bounties)
        if any(map(lambda e: e < 0, bounties)):
            return Score(-2, -1, [], False)
        player_stack = self.__get_player_stack__(seats, players)

        div = 5 if round(bounties_avg, 2) > stack else 4
        hand_stats = self.__parse_hand_stat__(gf, players, div)
        eliminates = list(map(lambda h: h.eliminate, hand_stats))
        win = any(map(lambda h: h.win, hand_stats))

        return Score(bounties_avg, player_stack, eliminates, win)

    @staticmethod
    def __get_seats__(gf):
        result = []
        with open(gf.path, encoding="utf8") as handler:
            line = handler.readline()
            while line and "Seat #1" not in line:
                line = handler.readline()
            if not line:
                logging.warning("File has no 'Seat #1', file={}, full path={}".format(gf.name, gf.path))
                return []
            line = handler.readline()
            while line and "Seat" in line:
                result.append(line)
                line = handler.readline()
            if len(result) != 4:
                logging.warning("File has {} seats, file={}, full path={}".format(len(result), gf.name, gf.path))
                return []

        return result

    def __parse_hand_stat__(self, gf, players, div):
        result = []
        hand_lines = []
        with open(gf.path, encoding="utf8") as handler:
            line = handler.readline()
            while line:
                line = line.strip()
                if line:
                    hand_lines.append(line)
                elif hand_lines:
                    result.append(self.__get_hand_stat__(hand_lines, players, div))
                    hand_lines.clear()
                line = handler.readline()
        if hand_lines:
            result.append(self.__get_hand_stat__(hand_lines, players, div))

        result = list(filter(lambda el: el, result))
        if any(map(lambda h: not h.eliminate.values, result)):
            logging.warning("File has weird eliminates, file={}, full path={}".format(gf.name, gf.path))
        return list(filter(lambda h: h.eliminate.values, result))

    def __get_hand_stat__(self, hand_lines, players, div):
        eliminate_lines = []
        for line in hand_lines:
            if "eliminating" in line:
                line_split = line.split()
                if line_split[0] in players:
                    eliminate_lines.append(line_split)
                    break
        if not eliminate_lines:
            return None

        bounties = {}
        try:
            for line in filter(lambda s: s.startswith("Seat"), hand_lines[:10]):
                player = line.strip().split()[2]
                bounty = self.__get_bounty__(line)
                bounties[player] = bounty

            win_bounties = []
            possible_bounties = []
            multipliers = []
            for eliminate_line in eliminate_lines:
                i = eliminate_line.index("wins")
                win_bounty = float(self.__get_num__(eliminate_line[i + 1]))
                j = eliminate_line.index("eliminating")
                bounty = bounties[eliminate_line[j + 1]]
                win_bounties.append(win_bounty)
                possible_bounties.append(bounty * 2 / div)
                multipliers.append(round(win_bounty / (bounty / div) * 10))

            win = False
            for line in reversed(hand_lines):
                if "wins the tournament" in line and line.split()[0] in players:
                    win = True

            return HandStat(Eliminate(multipliers, win_bounties, possible_bounties), win)
        except (ValueError, IndexError, KeyError):
            return HandStat(Eliminate([], [], []), False)

    def __get_bounty__(self, seat):
        try:
            parts = seat.strip().split()
            for i, part in enumerate(parts):
                if "bounty" in part and part.endswith(")") and "chip" in parts[i - 2]:
                    return float(self.__get_num__(parts[i - 1]))
            return -1
        except (ValueError, IndexError):
            return -1

    @staticmethod
    def __get_num__(s):
        i = 0
        while i < len(s) and not s[i].isdigit():
            i += 1
        return s[i:]

    def __get_player_stack__(self, seats, players):
        for seat in seats:
            try:
                parts = seat.split()
                if parts[2] in players:
                    bounty = self.__get_bounty__(seat)
                    i = parts.index("chips,")
                    chips = int(parts[i - 2][1:])
                    return Stack(bounty, chips)
            except (ValueError, IndexError):
                return Stack(-1, -1)
        return Stack(-1, -1)

    def __collect_files__(self, path):
        for root, _, files in os.walk(path):
            for file in files:
                path = os.path.join(root, file)
                if file.startswith("TS"):
                    fid = self.__get_file_id__(file, path)
                    self.stat_files.append(File(file, fid, path))
                    continue
                if file.startswith("HH") and "Varied" in file:
                    fid = self.__get_file_id__(file, path)
                    self.game_files.append(File(file, fid, path))
                    continue

    def __apply_mode__(self, mode):
        if mode:
            cut = len(self.game_files) // 20
            self.game_files = self.game_files[:cut]

    @staticmethod
    def __get_file_id__(file, full):
        parts = file.split()
        if len(parts) < 2:
            logging.warning("Cannot get id by name of file={}, full path={}".format(file, full))
        uid = parts[1]
        date = parts[0][2:]
        return "{}{}".format(date, uid)

    @staticmethod
    def __create_result_folder__(result, ranges):
        logging.info("Creating result folders...")
        range_map = {}
        i = 0
        date = datetime.now().strftime("%Y%m%d")
        result_folder = os.path.join(result, "result-{}-{}".format(date, i))
        while os.path.exists(result_folder):
            i += 1
            result_folder = os.path.join(result, "result-{}-{}".format(date, i))
        for r in ranges:
            path = os.path.join(result_folder, r.name)
            os.makedirs(path)
            range_map[r.max] = path

        range_map[INFO_FILE] = os.path.join(result_folder, "stat.txt")

        logging.info("Done creating result folders")
        return range_map


def parse_file_args(file):
    result = {}
    if os.path.exists(file):
        with open(file, encoding="utf8") as handler:
            config = handler.readlines()
        for k, v in map(lambda s: s.split("=", 1), config):
            result[k.strip()] = v.strip()

    return result


def parse_intervals(intervals):
    result = []
    values = sorted(map(float, map(lambda v: v.strip(), intervals.split(","))))
    for i in values:
        result.append(Range(float(i), str(i)))
    result.append(Range(MAX_SCORE, str(MAX_SCORE)))
    return result


def parse_player_names(names):
    return set(map(lambda name: name.strip().split()[0], names.split(",")))


def get_demo_key(s):
    result = []
    for i, el in enumerate(reversed(s)):
        if i % 3 == 0:
            result.append(el)
    return "".join(result)


def parse_args(config_file="config.txt"):
    parser = argparse.ArgumentParser(description="Group poker files")
    parser.add_argument("-p", "--path", metavar="--path", help="path to input folder")
    parser.add_argument("-r", "--result", metavar="result", help="path to result folder")
    parser.add_argument("-i", "--intervals", metavar="intervals", help="intervals to split files, e.g. 10,20,30")
    parser.add_argument("-d", "--demo", metavar="demo", help="turn off demo mode")
    parser.add_argument("-n", "--names", metavar="names", help="player names to calculate stat")
    parser.add_argument("-s", "--stack", metavar="stack", help="average stack to calculate stat")
    args = parser.parse_args()
    config_args = parse_file_args(config_file)

    if not args.path:
        if "path" not in config_args:
            raise ValueError("No path specified for input folder")
        else:
            args.path = config_args["path"]
    if not args.result:
        args.result = config_args.get("result", os.path.sep.join(args.path.split(os.path.sep)[:-1]))
    if not args.intervals:
        args.intervals = config_args.get("intervals", "15,28,60")
    args.ranges = parse_intervals(args.intervals)
    if not args.demo:
        args.demo = config_args.get("demo", "")
    if not args.names:
        args.names = config_args.get("names", "")
    args.players = parse_player_names(args.names)
    if not args.stack:
        args.stack = config_args.get("stack", "10.8")
    args.avg_stack = float(args.stack)
    args.mode = get_demo_key(args.demo) != "lkNFenco"

    return args


def split(path, result_folder, ranges, players, stack, mode):
    fs = FileStructure(path, mode)
    fs.split(result_folder, ranges, players, stack)


def main():
    args = parse_args()
    logging.info("Input folder: {}".format(args.path))
    logging.info("Result folder: {}".format(args.result))
    logging.info("Ranges: {}".format(args.ranges))
    logging.info("Player names: {}".format(args.players))
    logging.info("Avg stack: {}".format(args.avg_stack))
    logging.info("Demo mode: {}".format(args.mode))
    logging.info("")
    split(args.path, args.result, args.ranges, args.players, args.avg_stack, args.mode)

    return 0


if __name__ == "__main__":
    main()
