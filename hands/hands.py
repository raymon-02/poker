import argparse
import logging
import os
from collections import namedtuple
from datetime import datetime

FORMAT = "%(asctime)-15s [%(levelname)8s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

Hand = namedtuple("Hand", ["source", "seats"])
Seat = namedtuple("Seat", ["number", "chips"])

RESULT = "result"
REST = "rest"

LZN = 3
LZW = 2


class HandsStructure:
    def __init__(self, path):
        self.__files__ = []
        self.__collect_files__(path)

    def retrieve_hands(self, result, seats, chips, batch):
        logging.info("Start retrieving...")
        result_folder = self.__create_result_folder__(result)

        total = 0
        errors = 0
        hands_weird = []

        ni, nk = 0, 0
        result_file_normal = self.__file_name__(result_folder, RESULT, nk, LZN)
        handler_normal = open(result_file_normal, "w", encoding="utf-8")
        logging.info("Writing results into '{}'".format(result_file_normal))
        for hand in self.__get_hands__():
            total += 1
            if not hand:
                errors += 1
                continue
            if len(hand.seats) == seats and all(map(lambda seat: seat.chips == chips, hand.seats)):
                if sorted(map(lambda seat: seat.number, hand.seats)) == [i + 1 for i in range(seats)]:
                    ni += 1
                    for line in hand.source:
                        handler_normal.write("{}\n".format(line))
                    handler_normal.write("\n\n\n")
                    if ni % batch == 0:
                        nk += 1
                        handler_normal.close()
                        result_file_normal = self.__file_name__(result_folder, RESULT, nk, LZN)
                        handler_normal = open(result_file_normal, "w", encoding="utf-8")
                        logging.info("Writing results into '{}'".format(result_file_normal))
                else:
                    hands_weird.append(hand)
        handler_normal.close()

        if hands_weird:
            wk = 0
            for i in range(0, len(hands_weird), batch):
                result_file_weird = self.__file_name__(result_folder, REST, wk, LZW)
                logging.info("Writing rest into '{}'".format(result_file_weird))
                with open(result_file_weird, "w", encoding="utf-8") as handler_wired:
                    for hand in hands_weird[i:i + batch]:
                        for line in hand.source:
                            handler_wired.write("{}\n".format(line))
                        handler_wired.write("\n\n\n")

        logging.info("Done retrieving")
        logging.info("")
        logging.info("All hands     = {}".format(total + errors))
        logging.info("Handled hands = {}".format(total))
        logging.info("  normal      = {}".format(ni))
        logging.info("  weird       = {}".format(len(hands_weird)))
        logging.info("  filtered    = {}".format(total - ni - len(hands_weird)))
        logging.info("Error hands   = {}".format(errors))

    def __get_hands__(self):
        for file in self.__files__:
            yield from self.__parse_file__(file)

    def __parse_file__(self, file):
        hand_lines = []
        with open(file, encoding="utf-8") as handler:
            line = handler.readline()
            while line:
                line = line.strip()
                if line:
                    hand_lines.append(line)
                elif hand_lines:
                    yield self.__get_hand__(file, hand_lines)
                line = handler.readline()

        if hand_lines:
            yield self.__get_hand__(file, hand_lines)

    def __get_hand__(self, file, hand_lines):
        hand = self.__parse_hand__(hand_lines)
        if not hand:
            logging.warning(
                "Error parsing hand in file '{}' starting with '{}'".format(
                    file.split(os.path.sep)[-1],
                    hand_lines[0]
                )
            )
        hand_lines.clear()
        return hand

    def __parse_hand__(self, hand_lines):
        seats = []

        seats_found = False
        for line in hand_lines:
            if line.startswith("Seat"):
                seats_found = True
                seat = self.__parse_seat__(line)
                if not seat:
                    return None
                seats.append(seat)
            elif seats_found:
                break

        return Hand(hand_lines.copy(), seats) if seats else None

    @staticmethod
    def __parse_seat__(line):
        parts = line.split()
        if len(parts) < 2:
            return None

        ind = len(parts) + 10
        for i, part in enumerate(parts):
            if part.startswith("chip") and part.endswith(")") and i - 2 >= 0 and parts[i - 1] == "in":
                ind = i - 2
                break

        try:
            number = int(parts[1][:-1])
            chips = int(parts[ind][1:])
        except ValueError:
            return None

        return Seat(number, chips)

    def __collect_files__(self, path):
        for root, _, files in os.walk(path):
            for file in files:
                self.__files__.append(os.path.join(root, file))

    @staticmethod
    def __create_result_folder__(result):
        logging.info("Creating result folders...")
        i = 0
        date = datetime.now().strftime("%Y%m%d")
        result_folder = os.path.join(result, "result-{}-{}".format(date, i))
        while os.path.exists(result_folder):
            i += 1
            result_folder = os.path.join(result, "result-{}-{}".format(date, i))
        os.makedirs(result_folder)

        logging.info("Done creating result folders '{}'".format(result_folder))

        return result_folder

    @staticmethod
    def __file_name__(result_folder, name, k, zeros):
        return "{}{}{}-{}.txt".format(result_folder, os.path.sep, name, str(k).zfill(zeros))


def parse_file_args(file):
    result = {}
    if os.path.exists(file):
        with open(file, encoding="utf-8") as handler:
            config = handler.readlines()
        for k, v in map(lambda s: s.split("=", 1), config):
            result[k.strip()] = v.strip()

    return result


def parse_args(config_file="config.txt"):
    parser = argparse.ArgumentParser(description="Retrieve hands")
    parser.add_argument("-p", "--path", metavar="--path", help="path to input folder")
    parser.add_argument("-r", "--result", metavar="result", help="path to result folder")
    parser.add_argument("-s", "--seats", metavar="seats", help="number of seats", type=int)
    parser.add_argument("-c", "--chips", metavar="chips", help="chips for each seat", type=int)
    parser.add_argument("-b", "--batch", metavar="batch", help="batch size of result", type=int)
    args = parser.parse_args()
    config_args = parse_file_args(config_file)

    if not args.path:
        if "path" not in config_args:
            raise ValueError("No path specified for input folder")
        else:
            args.path = config_args["path"]
    if not args.result:
        args.result = config_args.get("result", os.path.sep.join(args.path.split(os.path.sep)[:-1]))
    if not args.seats:
        args.seats = int(config_args.get("seats", "6"))
    if not args.chips:
        args.chips = int(config_args.get("chips", "500"))
    if not args.batch:
        args.batch = int(config_args.get("batch", "1000"))

    return args


def retrieve_hands(path, result, seats, chips, batch):
    hs = HandsStructure(path)
    hs.retrieve_hands(result, seats, chips, batch)


def main():
    args = parse_args()
    logging.info("Input folder:  {}".format(args.path))
    logging.info("Result folder: {}".format(args.result))
    logging.info("Seats:         {}".format(args.seats))
    logging.info("Chips:         {}".format(args.chips))
    logging.info("Batch size:    {}".format(args.batch))
    logging.info("")

    retrieve_hands(args.path, args.result, args.seats, args.chips, args.batch)

    return 0


if __name__ == "__main__":
    main()
