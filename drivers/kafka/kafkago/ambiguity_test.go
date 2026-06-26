package kafkago_test

// the ambiguous outcome, which is the one thing a caller cannot resolve on
// its own: the record may be in the log.
//
// It is WIDER on this client than on franz-go and the reason is worth stating
// once. kafka-go's WriteMessages returns the caller's ctx.Err() the moment the
// context is done and lets the produce carry on, on a goroutine of the
// writer's, with a deadline of its own — its own documentation says "there are
// no guarantees made on whether messages were written to kafka, they might
// also still be written after this method has already returned". And because
// the writer sends no producer id, a retry inside the writer's own budget can
// leave a SECOND copy that the broker does not de-duplicate
// , which is what DuplicablePublications counts.
