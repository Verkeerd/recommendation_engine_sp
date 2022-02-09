def palindrome_check(string):
    """
    takes a string as input. Checks if the string is a palindrome (a word that remains the same when you reverse it
    i.e. noon). returns True when the string is a palindrome, otherwise returns False.
    args:
        :param string: (str) a possible palindrome
    returns:
        :return: (bool) True when the string is a palindrome, otherwise False.
    """
    len_s = len(string)
    if len_s == 1 or len_s == 2:
        return True
    if string[0] == string[-1]:
        return palindrome_check(string[1:-1])
    else:
        return False


print(palindrome_check('noon'))
print(palindrome_check('nomon'))
print(palindrome_check('nomnon'))
