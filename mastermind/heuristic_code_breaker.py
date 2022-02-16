import general_functions as gf
import statistics


def best_guess(codes_to_check, all_codes, possible_pegs):
    """
    Takes all codes to consider as guess (list) [str]; all potential codes (list) [str]; potential_pins (int) as input.
    Calculates the amount of codes left after all possible feedback (combination of white and black pins) that can be
    received. For every code, does the following:
    For every possible feedback, checks how many codes would be left over after.
    Puts these values in a list. Calculates the standard deviance of this list.
    Returns the code (list) [int] that gave the lowest standard deviance.
    """
    print('I\'m thinking about my next move...')

    best_code = []
    best_score = 6 ** 4

    for code in codes_to_check:
        list_codes_left_over = []

        for peg_outcome in possible_pegs:
            codes_left_over = len(gf.eliminate_codes(list(code), all_codes, peg_outcome))
            list_codes_left_over.append(codes_left_over)
        code_score = statistics.stdev(list_codes_left_over)
        # if the worst-case of code is better than previously recorded best worst-case
        # records new best worst-case. records new best code.
        if code_score < best_score:
            best_code = code
            best_score = code_score

    return list(best_code)


def first_guess(all_codes, slots, possible_pegs):
    """
    Takes all potential codes (list) [str]; slots (int); possible_pegs as input.
    Calculates the best first guess based on the lowest standard deviance (see best_guess).
    All colours can be treated as the same in the first guess, as there is no statistical difference
    between     blue    blue    blue    red
    and         yellow  yellow  yellow  red
    Returns the best first guess (list) [int].
    """
    starters = gf.get_starters(slots)

    return best_guess(starters, all_codes, possible_pegs)


def heuristic_code_breaker():
    """
    Prints information, generates all possible codes slots long with all entries a number between 0 and colour_range.
    Plays mastermind with the user by guessing the code and asking for feedback. Does this until the bot breaks the
    code. Finally, gives the user the option to play again or quit.
    """
    # asks user for amount of slots and amount of colour-options.
    slots, colour_range = gf.set_slots_and_colours(max_slots=4, max_colours=6)
    possible_pegs = gf.all_possible_pins(slots=slots)
    if not slots:
        gf.goodbye_message()
        return None
    # prints information and waits for the user
    input("""
##########################################################################################
Hello! I am the Heuristic Code Breaker.
I am excited to be playing against you today!

First I'll explain the rules:

You have pegs in multiple colours and spots to place them on.
You can use the same colour on multiple spots, and the order matters!
This is the list with all possible colours:
    - red (r)
    - blue (b)
    - green (g)
    - yellow (y)
    - purple (p)
    - orange (o)
    - white (w)
    - teal (t)

When you have created the combination, I will attempt to guess it.
After I guess, please provide me with feedback!

Give me a red pin for every peg that is the correct colour and in the correct spot.
Give a white pin for every peg that is the the correct colour, but not in the correct spot.

Every peg in your code can only award one pin. Red feedback_pins have priority over white feedback_pins.
(e.g. if your code is:   Blue,   Blue,   Green,  Green
and I guess:             Blue,   Green,  Green,  Green,
I will receive two red feedback_pins and no white ones.
The second peg in my guess (green) will not receive a white peg, because both green pegs
in your code already awarded a pin.)


Those are the rules!
If you are confused about anything, please look up the official mastermind rules at
https://www.ultraboardgames.com/mastermind/game-rules.php


Please create a secret code!
Remember it well, or write it down somewhere.

Press Enter to continue
""")
    possible_codes = gf.all_possible_codes(slots=slots, colour_range=colour_range)

    rounds = 1

    guess = first_guess(possible_codes, slots, possible_pegs)
    # prints the guess in a user-friendly format
    print("""
##########################################################################################

Let's begin!

My first guess is {}
""".format(gf.format_colours(guess)))

    # asks user for feedback.
    feedback_pins = gf.input_pin_feedback(slots)
    if not feedback_pins:
        gf.goodbye_message()
        return None

    while feedback_pins != (slots, 0):
        rounds += 1
        # eliminates all combination no longer possible with the provided feedback
        possible_codes = gf.eliminate_codes(guess=guess, all_codes=possible_codes, feedback_pins=feedback_pins)

        if not possible_codes:
            print('I\'m all out of guesses!\nPlease review your feedback to make sure you didn\'t make a mistake.')
            return None

        # calculates the next best guess (based on the best worst-case scenario)
        guess = best_guess(possible_codes, possible_codes, possible_pegs)
        print(guess)
        print("My {}nd guess is {}\n".format(rounds, gf.format_colours(guess)))

        # asks user for feedback
        feedback_pins = gf.input_pin_feedback(slots)
        if not feedback_pins:
            gf.goodbye_message()
            return None

    # program quits automatically if user doesn't want to continue
    if input("""
Yay! I guessed the code.
It was {}
It took me {} rounds to guess it.

##########################################################################################
1) Play again
2) Open selection menu
""".format(gf.format_colours(guess), rounds)).strip() == '1':
        heuristic_code_breaker()
