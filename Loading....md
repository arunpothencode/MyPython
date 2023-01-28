

def palindrome(word): 
    rev_word = word[::-1] 
    if (word == rev_word): 
        return True
    return False
  
# Driver code 
word = "malayalam"
ans = palindrome(word) 
  
if ans == 1: 
    print("Yes") 
else: 
    print("No")