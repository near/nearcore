## Goal of this doc

We rely heavily on asynchronous conversation for our day-to-day work via Zulip/Slack/Github. As a result, how we communicate on these asynchronous collaboration tools plays a critical role on our productivity and efficiency. 

In this doc, we go over two scenarios we often face in our daily interaction, list outstanding problems and propose simple ways to resolve them. The proposal is not set in stone and any suggestions and criticisms are welcome as always.


## TL;DR;

Before jumping into scenarios, here is TL;DR; of the proposal.

* **‘Ack’ a message after reading it (with emoji or comment)**: I am sure you have watched a YouTube video, where a YouTuber asks you to ‘like’ their video. Set aside how YouTube algorithm works with number of likes on a video, knowing that other people saw my content can be very fulfilling and heart-warming. Acknowledging someone’s message with emoji is a simple yet powerful way to show your respect and appreciation to the author who must have given lots of thoughts for their message.

* **Be explicit about ‘whom you want to get feedback from’**: All of us are busy with our own works and given a limited time, it is not feasible to check out every single on-going conversation. By tagging specific people, you not only help other people spend their time on something more relevant to them, but also can get the response faster. Besides, this is yet another way to send the gesture that their opinions are appreciated.

* **As a topic owner, help facilitate the conversation**: 
  * Make sure the thread is razor focused on the main topic and cut the potential detour as early as possible. If a new discussion point is worth continuing, move the relevant messages to a new topic so the discussion can continue in a different channel.
  * Occasionally summarize the thread so everyone can get up to speed quickly
  * Set the timeline expectation on how you want to make a progress in the discussion and guide people accordingly.
  * Once conclusion is reached, share it and resolve the topic. Do not leave it open unnecessarily.


## Scenario based deep-dive

### Scenario 1. Walnut wants to share updates/announcements/news with team members.

Walnut sends a message on Zulip/Slack.

*“Hello everyone! we have a new guideline on our engineering practice and want to make sure everyone have a look. Here is the link! Don’t hesitate to leave a comment if you have any suggestion/question. Thanks!”*

After seeing the message, team members opens the link and read the doc. As everything looks good, no one leaves a comment.
* **Potential issue**: As none of the tool we use has a way for Walnut to figure out who read their message, Walnut starts wondering whether their message was delivered.
  * **Proposal**: Leave a comment or add an emoji to ‘ack’ the message.

### Scenario 2. Nala has an idea and wants to have technical discussion with other engineers to collect feedback and reach to the conclusion.

Nala starts a new topic on Zulip regarding a storage issue.

*“I have a new idea to address the storage issue we have been having recently. Here is how it goes. …[idea overview]… But there are several outstanding problems with this approach. … [problem statement]… And here are the potential way to resolve them. …[solution]… I would like to get some feedback from the team and see if we can get aligned.”*
* **Potential issue**: potential stakeholders may not subscribe the stream where the message is posted and never participate in the discussion.
  * **Proposal**: Tag people/team whom you want to participate in the discussion or who you think will find the discussion valuable. Pay special attention to the way you tag them though, to not make the topic ‘exclusive’ to those people. On the other hand, avoid using @ all or @ everyone tag as that can be spammy and too broad.

The message sits there for two days, some people reads the message, but do not participate.
* **Potential issue**: As Nala’s message did not receive any response or confirmation of being read, Nala starts wondering whether no one has read the message or the topic is not interesting to anyone.
  * **Proposal**: ‘Ack’ the message by adding an emoji or leave comments. You don’t need to add emoji for every single on-going conversation. Rather, adding an emoji at the last message of the latest thread will do. 

Eventually, Maukoo participates in the discussion.

*“Hey! Thanks for the great suggestion. Overall it looks good, but I can see several corner cases. …[list of corner cases]… How about we try this? …[alternative suggestion]… This way, I think we can meet in the middle ground and everything should work as expected.”*

Then network expert Coriander participates as well.

*“Maukoo’s idea will mostly work, but it introduces additional loads on network. …[issue description]… In the past, we saw the similar issues from other flows and that caused this other problems. …[other problems caused by additional load on network]”*

Another person Simba also contributes.

*“Oh, I remember that network issue. I wonder if we can do this. …[suggesting solution]…”* 
* **Potential issue**: Discussion is deviated from the original topic and the thread becomes harder to follow.
  * **Proposal**: Point out that the conversation is starting to diverge and try to bring the crowd back to the main topic. If the newly presented topic is worth a discussion, it can be done in a separate thread.

The topic discussion is ongoing for several days. Nala has to take care of other matters so decides to leave the discussion open until they are done with the other matters.
* **Potential issue**: As the discussion gets long, it can be overwhelming for a new person to participate if they have to read 50 previous messages.
  * **Proposal**: Occasionally summarizing the discussion up to certain point can be helpful for everyone to quickly gather/refresh context. If it is an important discussion that better be documented, consider creating a Google doc so discussion points can be structured and archived.

* **Potential issue**: As other participants also have their own jobs to do, they may quickly lose their interest and forget about the discussion.
  * **Proposal**: As someone who started the topic, it is your responsibility to make sure that the topic thread stays alive. If you cannot get back to this anytime soon, let the stakeholders know and share when you plan to pick up the discussion again so everyone can set the right expectation.

Some time later, Nala asynchronously works with other engineers and align on the solution.
* **Potential issue**: From the Zulip topic point of view, the topic is never resolved and discussion is dropped in the middle. As a result, people who were not part of the offline discussion do not know the final state of the topic.
  * **Proposal**: Set the high level timeline on when the discussion needs to reach to the next stage (e.g. collect feedback in week 1, finalize solution in week 2, resolve the topic in week 3) and update the topic thread accordingly. Once the discussion is completed, share the outcome and resolve the topic thread.
