## Problems

There are currently problems with the approach that are difficult
to ignore

1) hallucinations 
2) context
3) looping

### Hallucinations
At times the agent tells us things that aren't true - 
that's not good for something writing code.

A rough example might go as follows

````example
me: the code you wrote is not compiling 
    i'm using libX v1.0.1 
    can you check the API for that function on the struct?

agent: I've checked the api and that method definitely exists
````
I need to paste the correct implementation into the context to convince the agent it has made this up...

This is not uncommon - the agent *seems* so competent that you get used to staying within its scope - 
often asking it to debug itself solves the problem so you can get locked into loops before deciding to go out and check 
for yourself.

This is in itself problematic - that trust in the Agent makes you lazy - of course that may be long term goal for us - 
we don't necessarily need to know all the details if things work (security etc become a separate concern here)

### Context

The context is surprisingly mercurial - this goes unnoticed until things go wrong - 
the context does not seem to have a linear idea of time - 
this may be part of its successful ability to keep track of long distance relationships between ideas.

````example
me: the integration test is working now can you now address x ... (I add more context)

agent: ok I understand your frustration that the integration test is not working ... (long digression)

me: you seem confused - can you repeat what I asked you in my last prompt

agent: (picks a moment in the interaction where I point out a problem with the integration test)
````

again this is an example of a problem with how we are fooled(?) into thinking of the Agent 
as being human like - it is not thinking like us - it does not have a linear memory, 
it approaches the whole thing in a different way and only seems to interact in a human way

### looping

this seems most evident if the Agent creates problematic code which we try and work with the Agent to refactor.

whatever there is that made the Agent choose that problematic code can be very hard to override

````example
me: let's correct the client, we need it to connect over http for the integration test

agent: ok we'll change ...
(problem successfully corrected)

.. time passes

me: we'll add a test to check x ... (some unconnected test of function)

agent: I've added that test

me: but you've broken our agent, the client now has breaking options...

agent: ok we'll change ...
(problem successfully corrected)

me: we'll add a test to check x ... (some unconnected test of function)

agent: I've added that test

me: but you've broken our agent AGAIN, the client now has breaking options...

agent: I'm so sorry this must be frustrating: we'll change ...
(problem successfully corrected)

````

and this problem keeps coming back - at one point I ask:

````example
me: can you confirm with me before editing func clientCreationFunction()
    I *never* want it edited
    
agent: ok I'll be careful not to edit clientCreationFunction 
````

and a few prompts later it goes ahead and edits the func

again it's human like response fools us into thinking it really knows what we're asking

it doesn't
