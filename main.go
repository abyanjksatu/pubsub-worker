package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "docker-220612")
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}

	// Print all the subscriptions in the project.
	fmt.Println("Listing all subscriptions from the project:")
	subs, err := list(client)
	if err != nil {
		log.Fatal(err)
	}
	for _, sub := range subs {
		fmt.Println(sub)
	}

	t := createTopicIfNotExists(client)

	const sub = "kecci"
	// Create a new subscription.
	if err := create(client, sub, t); err != nil {
		log.Println(err)
	}

	// Pull messages via the subscription.
	if err := pullMsgs(client, sub, t); err != nil {
		log.Fatal(err)
	}

	// Delete the subscription.
	// if err := delete(client, sub); err != nil {
	// 	log.Fatal(err)
	// }
}

func list(client *pubsub.Client) ([]*pubsub.Subscription, error) {
	ctx := context.Background()
	// [START pubsub_list_subscriptions]
	var subs []*pubsub.Subscription
	it := client.Subscriptions(ctx)
	for {
		s, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	// [END pubsub_list_subscriptions]
	return subs, nil
}

func pullMsgs(client *pubsub.Client, subName string, topic *pubsub.Topic) error {
	fmt.Println("here.....")
	ctx := context.Background()
	sub := client.Subscription(subName)
	sub.ReceiveSettings.NumGoroutines = 1
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Println("message id ", msg.ID)
		fmt.Printf("Got message: %q\n", string(msg.Data))
		msg.Ack()
	})

	if err != nil {
		log.Println(err.Error())
	}

	return nil
}

func pullMsgsError(client *pubsub.Client, subName string) error {
	ctx := context.Background()
	// [START pubsub_subscriber_error_listener]
	// If the service returns a non-retryable error, Receive returns that error after
	// all of the outstanding calls to the handler have returned.
	err := client.Subscription(subName).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Printf("Got message: %q\n", string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		return err
	}
	// [END pubsub_subscriber_error_listener]
	return nil
}

func pullMsgsSettings(client *pubsub.Client, subName string) error {
	ctx := context.Background()
	// [START pubsub_subscriber_flow_settings]
	sub := client.Subscription(subName)
	sub.ReceiveSettings.MaxOutstandingMessages = 10
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Printf("Got message: %q\n", string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		return err
	}
	// [END pubsub_subscriber_flow_settings]
	return nil
}

func create(client *pubsub.Client, subName string, topic *pubsub.Topic) error {
	ctx := context.Background()
	// [START pubsub_create_pull_subscription]
	sub, err := client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created subscription: %v\n", sub)
	// [END pubsub_create_pull_subscription]
	return nil
}

func createWithEndpoint(client *pubsub.Client, subName string, topic *pubsub.Topic, endpoint string) error {
	ctx := context.Background()
	// [START pubsub_create_push_subscription]

	// For example, endpoint is "https://my-test-project.appspot.com/push".
	sub, err := client.CreateSubscription(ctx, subName, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Second,
		PushConfig:  pubsub.PushConfig{Endpoint: endpoint},
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created subscription: %v\n", sub)
	// [END pubsub_create_push_subscription]
	return nil
}

func updateEndpoint(client *pubsub.Client, subName string, endpoint string) error {
	ctx := context.Background()
	// [START pubsub_update_push_configuration]

	// For example, endpoint is "https://my-test-project.appspot.com/push".
	subConfig, err := client.Subscription(subName).Update(ctx, pubsub.SubscriptionConfigToUpdate{
		PushConfig: &pubsub.PushConfig{Endpoint: endpoint},
	})
	if err != nil {
		return err
	}
	fmt.Printf("Updated subscription config: %#v", subConfig)
	// [END pubsub_update_push_configuration]
	return nil
}

func delete(client *pubsub.Client, subName string) error {
	ctx := context.Background()
	// [START pubsub_delete_subscription]
	sub := client.Subscription(subName)
	if err := sub.Delete(ctx); err != nil {
		return err
	}
	fmt.Println("Subscription deleted.")
	// [END pubsub_delete_subscription]
	return nil
}

func createTopicIfNotExists(c *pubsub.Client) *pubsub.Topic {
	ctx := context.Background()

	const topic = "my-topic"
	// Create a topic to subscribe to.
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		return t
	}

	t, err = c.CreateTopic(ctx, topic)
	if err != nil {
		log.Fatalf("Failed to create the topic: %v", err)
	}
	return t
}

func getPolicy(c *pubsub.Client, subName string) (*iam.Policy, error) {
	ctx := context.Background()

	// [START pubsub_get_subscription_policy]
	policy, err := c.Subscription(subName).IAM().Policy(ctx)
	if err != nil {
		return nil, err
	}
	for _, role := range policy.Roles() {
		log.Printf("%q: %q", role, policy.Members(role))
	}
	// [END pubsub_get_subscription_policy]
	return policy, nil
}

func addUsers(c *pubsub.Client, subName string) error {
	ctx := context.Background()

	// [START pubsub_set_subscription_policy]
	sub := c.Subscription(subName)
	policy, err := sub.IAM().Policy(ctx)
	if err != nil {
		return err
	}
	// Other valid prefixes are "serviceAccount:", "user:"
	// See the documentation for more values.
	policy.Add(iam.AllUsers, iam.Viewer)
	policy.Add("group:cloud-logs@google.com", iam.Editor)
	if err := sub.IAM().SetPolicy(ctx, policy); err != nil {
		return err
	}
	// NOTE: It may be necessary to retry this operation if IAM policies are
	// being modified concurrently. SetPolicy will return an error if the policy
	// was modified since it was retrieved.
	// [END pubsub_set_subscription_policy]
	return nil
}

func testPermissions(c *pubsub.Client, subName string) ([]string, error) {
	ctx := context.Background()

	// [START pubsub_test_subscription_permissions]
	sub := c.Subscription(subName)
	perms, err := sub.IAM().TestPermissions(ctx, []string{
		"pubsub.subscriptions.consume",
		"pubsub.subscriptions.update",
	})
	if err != nil {
		return nil, err
	}
	for _, perm := range perms {
		log.Printf("Allowed: %v", perm)
	}
	// [END pubsub_test_subscription_permissions]
	return perms, nil
}
