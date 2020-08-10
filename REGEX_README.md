### Regex definition rules ###

In order to define a function-matching regex, following rules on groups must be satisfied

`[1]` group 1: name of the function

`[2]` group 2: nested function plus comma and optional separating char. If not present, the match will extract null.

`[3]` group 3: 'pure' nested function. If not present, the match will extract null. 
Use `@` if the transformation must be applied directly on the current column.

Then, a group for each parameter of function identified by group 1, if any. Thus

`[4]` group 4: first parameter of function identified by group 1

`[5]` group 5: second parameter of function identified by group 1

and so on ... 

A concrete example:
    
       lpad function. It takes two parameters (length and padding character).
    
       [1] lpad(@, 5, '0') -> no nested function
       [2] lpad(substring(0, 3), 5, '0') -> nested function:
    
       A signature able to match both definitions is ^(lpad)\(((.+),\s)?(\d+),\s'(.+)'\)$. 
       Let's analyze it
    
       Group 1 (function name): (lpad). It will capture string 'lpad' in both cases
       Group 2 (nested function plus comma and spacing char): ((.+),\s). It will capture '@, ' in the first case, while 'substring(0, 3), ' in the second one
       Group 3 (nested function): (.+). Defined within Group 2. It will capture '@' in the first case, 'substring(0, 3)' in the second one
       Group 4 (first argument of function): (\d+). It will capture string '5' in both cases
       Group 5 (second argument of function): (.+). It will capture string '0' in both cases