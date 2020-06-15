### Regex definition rules ###

`[a]` For a function that has parameters:

`[a.1]` Group 1: name of the function

`[a.2]` Group 2: nested function plus comma and separating char (to be marked as optional). 
If not present, the match will extract null.

`[a.3]` Group 3: nested function. If not present, the match will extract null

Then, a group for each parameter of function identified by Group 1, if any. Thus

`[a.4]` Group 4: first parameter of function identified by Group 1

`[a.5]` Group 5: second parameter of function identified by Group 1

and so on ... )

Example:
    
       lpad function. It takes two parameters (length and padding character).
    
       [1] lpad(5, '0') -> no nested function
       [2] lpad(substring(0, 3), 5, '0') -> nested function:
    
       A signature able to match both definitions is ^(lpad)\(((.+),\s)?(\d+),\s'(.+)'\)$. 
       Let's analyze it
    
       Group 1 (function name): (lpad). It will capture string 'lpad' in both cases
       Group 2 (nested function plus comma and spacing char): ((.+),\s). It will capture string 'substring(0, 3), ' in second case
       Group 3 (nested function): (.+). Defined within GROUP 2. It will capture string 'substring(0, 3)' in second case
       Group 4 (first argument of function): (\d+). It will capture string '5' in both cases
       Group 5 (second argument of function): (.+). It will capture string '0' in both cases

`[b]` For a function that has no parameters:

`[b.1]` Group 1: name of the function

`[b.2]` Group 2: nested function (to be marked as optional). 
If not present, the match will extract null.

Example:

     look_up_istituto function. It takes no parameters

     [1] look_up_istituto() -> no nested function
     [2] look_up_istituto(lpad(5, '0')) -> nested function

     A signature able to match both definitions is ^(look_up_istituto)\((.+)?\)$. 
     Let's analyze it

     Group 1 (function name): (look_up_istituto). It will capture string 'look_up_istituto' in both cases
     Group 2 (nested function): (.+). It will capture string 'lpad(5, '0')' in second case