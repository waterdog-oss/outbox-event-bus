package mobi.waterdog.eventbus.model

/**
 * Defines how to deal with consumer offset commits
 * */
enum class StreamMode {
    AutoCommit,         //Use kafka's auto commit
    EndOfBatchCommit,   //Commit at the end of each batch of messages
    MessageCommit       //Commit whenever a single message is processed
}