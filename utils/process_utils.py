import re

re_pattern = re.compile(
    r"\d+\.\s(\w+\d)\s\{\s\[%eval\s([-+]?\d+(?:\.\d+)?)\]\s\[%clk\s(\d+:\d+:\d+)\]\s\}\s(?:\d+\.+\s)?(\w+\d)\s\{\s\[%eval\s([-+]?\d+(?:\.\d+)?)\]\s\[%clk\s(\d+:\d+:\d+)\]\s\}"
)


def parse_game_moves(game_string: str) -> list[dict]:
    """
    Parse the game string to extract the moves and evaluations.

    Args:
    game_string: str
        The string containing the game moves.
    game_id: int
        The id of the game.

    Returns:
    list[dict]: A list of dictionaries containing the parsed moves and evaluations.
    """
    moves = re_pattern.findall(game_string)
    return [
        {
            "White Move": white_move,
            "White Evaluation": float(white_eval),
            "White Time": white_time,
            "Black Move": black_move,
            "Black Eval": float(black_eval),
            "Black Time": black_time,
        }
        for white_move, white_eval, white_time, black_move, black_eval, black_time in moves
    ]
