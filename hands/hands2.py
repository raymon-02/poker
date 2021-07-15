import argparse
import logging
import os
from collections import namedtuple
from datetime import datetime

FORMAT = "%(asctime)-15s [%(levelname)8s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

Hands = namedtuple("Hands", ["normal", "weird", "total", "errors"])
Hand = namedtuple("Hand", ["source", "seats"])
Seat = namedtuple("Seat", ["number", "chips"])


class HandsStructure:
    def __init__(self, path):
        self.__files__ = []
        self.__collect_files__(path)

    def retrieve_hands(self, result, seats, chips, batch):
        logging.info("Start retrieving...")
        result_folder = self.__create_result_folder__(result)

        hands_normal = []
        hands_weird = []
        total = 0
        errors = 0

        for file in self.__files__:
            hands = self.__get_hands__(file, seats, chips)
            hands_normal.extend(hands.normal)
            hands_weird.extend(hands.weird)
            total += hands.total
            errors += hands.errors

        k = 0
        lz = len(str(len(hands_normal) // batch + 1))
        for i in range(0, len(hands_normal), batch):
            result_file_normal = "{}{}{}-{}.txt".format(result_folder, os.path.sep, "result", str(k).zfill(lz))
            logging.info("Writing results into '{}'".format(result_file_normal))
            with open(result_file_normal, "w", encoding="utf-8") as handler:
                for hand in hands_normal[i:i + batch]:
                    for line in hand.source:
                        handler.write("{}\n".format(line))
                    handler.write("\n\n\n")
            k += 1

        if hands_weird:
            k = 0
            lz = len(str(len(hands_weird) // batch + 1))
            for i in range(0, len(hands_weird), batch):
                result_file_weird = "{}{}{}-{}.txt".format(result_folder, os.path.sep, "rest", str(k).zfill(lz))
                logging.info("Writing rest into '{}'".format(result_file_weird))
                with open(result_file_weird, "w", encoding="utf-8") as handler:
                    for hand in hands_weird[i:i + batch]:
                        for line in hand.source:
                            handler.write("{}\n".format(line))
                        handler.write("\n\n\n")

        logging.info("Done retrieving")
        logging.info("")
        logging.info("All hands     = {}".format(total + errors))
        logging.info("Handled hands = {}".format(total))
        logging.info("  normal      = {}".format(len(hands_normal)))
        logging.info("  weird       = {}".format(len(hands_weird)))
        logging.info("  filtered    = {}".format(total - len(hands_normal) - len(hands_weird)))
        logging.info("Error hands   = {}".format(errors))

    def __get_hands__(self, file, seats, chips):
        hands, errors = self.__parse_file__(file)
        normal = []
        weird = []
        for hand in hands:
            if len(hand.seats) == seats and all(map(lambda seat: seat.chips == chips, hand.seats)):
                if sorted(map(lambda seat: seat.number, hand.seats)) == [i + 1 for i in range(seats)]:
                    normal.append(hand)
                else:
                    weird.append(hand)

        return Hands(normal, weird, len(hands), errors)

    def __parse_file__(self, file):
        result = []
        errors = 0

        with open(file, encoding="utf-8") as handler:
            lines = handler.readlines()
        lines.append(" ")

        hand_lines = []
        for line in lines:
            line = line.strip()
            if line:
                hand_lines.append(line)
            elif hand_lines:
                hand = self.__parse_hand__(hand_lines)
                if hand:
                    result.append(hand)
                else:
                    errors += 1
                    logging.warning(
                        "Error parsing hand in file '{}' starting with '{}'".format(
                            file.split(os.path.sep)[-1],
                            hand_lines[0]
                        )
                    )
                hand_lines.clear()

        return result, errors

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
