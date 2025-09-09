defmodule Proca.Server.MTTWorkerTest do
  use Proca.DataCase

  import Ecto.Query
  import Proca.Repo

  import Proca.StoryFactory, only: [green_story: 0]
  alias Proca.Factory

  alias Proca.Server.MTTWorker
  alias Proca.Action.Message

  use Proca.TestEmailBackend
  use Proca.TestProcessing

  setup do
    green_story()
  end

  test "3 days campaign for 8 hours each day is 24 cycles", %{campaign: c} do
    assert MTTWorker.calculate_cycles(c) == {1, 16}
  end

  test "We are in sending window", %{campaign: c} do
    assert MTTWorker.within_sending_window(c)
  end

  # describe "storing contact fields in supporter" do
  #   test "last_name", %{campaign: c} do
  #   end
  # end

  describe "selecting targets to send" do
    setup %{campaign: c, ap: ap, targets: ts} do
      action1 =
        Factory.insert(:action,
          action_page: ap,
          processing_status: :delivered,
          supporter_processing_status: :accepted,
          testing: true
        )

      action2 =
        Factory.insert(:action,
          action_page: ap,
          processing_status: :delivered,
          supporter_processing_status: :accepted
        )

      {t1, t2} = Enum.split(ts, 3)

      msg1 = Enum.map(t1, &Factory.insert(:message, action: action1, target: &1))
      msg2 = Enum.map(t2, &Factory.insert(:message, action: action2, target: &1))

      %{
        test_messages: msg1,
        live_messages: msg2
      }
    end

    test "return all testing mtts at once", %{campaign: c, ap: ap, targets: ts, test_messages: test_messages, live_messages: live_messages} do
      limit_messages_per_hour = MTTWorker.max_messages_per_hour(c)

      tids = MTTWorker.get_sendable_target_ids(c)
      assert length(tids) == 10

      emails = Proca.Repo.all(MTTWorker.query_test_emails_to_delete())
      assert length(emails) == 0

      emails = MTTWorker.get_test_emails_to_send()
      assert length(emails) == test_messages |> length()

      # Before dupe rank was run:
      emails = MTTWorker.get_emails_to_send(tids, {700, 700}, limit_messages_per_hour)
      assert length(emails) == 0

      assert {:ok, _} = Proca.Server.MTT.dupe_rank()

      emails = MTTWorker.get_emails_to_send(tids, {1, 700}, limit_messages_per_hour)
      assert length(emails) == 0

      emails = MTTWorker.get_emails_to_send(tids, {700, 700}, limit_messages_per_hour)
      assert length(emails) == live_messages |> length()

      # we have 3 test emails and 7 live emails (one per target), so at 699 we still do not send that one i guess?
      emails = MTTWorker.get_emails_to_send(tids, {699, 700}, limit_messages_per_hour)
      assert length(emails) == 0
    end
  end

  describe "scheduling messages for one target" do
    setup %{campaign: c, ap: ap, targets: [t1 | _]} do
      actions =
        Factory.insert_list(1000, :action,
          action_page: ap,
          processing_status: :delivered,
          supporter_processing_status: :accepted
        )

      msgs = Enum.map(actions, &Factory.insert(:message, action: &1, target: t1))

      Proca.Server.MTT.dupe_rank()

      %{
        actions: actions,
        messages: msgs,
        target: t1
      }
    end

    test "sending on schedule with injected time", %{actions: actions, campaign: c, target: %{id: tid}} do
      start_at = ~U[2025-09-05 09:00:00Z]
      end_at = ~U[2025-09-07 17:00:00Z] # 3 days, 8 hours per day = 24 cycles.

      # To avoid flakiness from `max_messages_per_hour`, which can depend on the
      # current time, we set a high limit on the campaign itself. This makes
      # the test focus on the throttling logic.
      mtt = Proca.Repo.get_by!(Proca.MTT, campaign_id: c.id)

      mtt_changeset =
        Proca.MTT.changeset(mtt, %{
          start_at: start_at,
          end_at: end_at,
          max_messages_per_hour: 100
        })

      Proca.Repo.update!(mtt_changeset)

      campaign = Proca.Campaign.one(id: c.id, preload: [:mtt])

      # First hour of campaign
      now1 = ~U[2025-09-05 09:01:00Z]
      limit1 = 80

      {cycle1, all_cycles1} = MTTWorker.calculate_cycles(campaign, now1)
      assert {cycle1, all_cycles1} == {1, 24}
      emails1 = MTTWorker.get_emails_to_send([tid], {cycle1, all_cycles1}, limit1)
      assert length(emails1) == 41
      Message.mark_all(emails1, :sent)

      # Middle of campaign
      now2 = ~U[2025-09-06 12:01:00Z]
      limit2 = 70

      {cycle2, all_cycles2} = MTTWorker.calculate_cycles(campaign, now2)
      assert {cycle2, all_cycles2} == {12, 24}
      emails2 = MTTWorker.get_emails_to_send([tid], {cycle2, all_cycles2}, limit2)
      assert length(emails2) == limit2
      Message.mark_all(emails2, :sent)

      # End of campaign - send the rest
      now3 = ~U[2025-09-07 16:01:00Z]
      limit3 = 90

      {cycle3, all_cycles3} = MTTWorker.calculate_cycles(campaign, now3)
      assert {cycle3, all_cycles3} == {24, 24}
      emails3 = MTTWorker.get_emails_to_send([tid], {cycle3, all_cycles3}, limit3)
      assert length(emails3) == limit3
      Message.mark_all(emails3, :sent)
    end

    test "test sending", %{campaign: c, target: %{id: tid, emails: [%{email: email}]}} do
      import Ecto.Query
      Proca.Repo.update_all(from(a in Proca.Action), set: [testing: true])

      test_email = "testemail@proca.app"
      c = %{c | mtt: Proca.Repo.update!(change(c.mtt, %{test_email: test_email}))}

      MTTWorker.process_mtt_test_mails()

      mbox = Proca.TestEmailBackend.mailbox(test_email)

      # limit to one per locale!
      assert length(mbox) == 1

      msg = mbox |> List.first()

      assert String.starts_with?(msg.subject, "[TEST]")
      assert msg.cc == [{"", test_email}]
    end

    test "live sending", %{campaign: c, target: %{id: tid, emails: [%{email: email}]}} do
      msgs = MTTWorker.get_emails_to_send([tid], {1, 1})

      assert Enum.all?(msgs, fn %{
                                  action: %{
                                    supporter: %{
                                      last_name: ln
                                    }
                                  }
                                } ->
               ln != nil
             end)

      MTTWorker.send_emails(c, msgs)

      te = Proca.TargetEmail.one(target_id: tid)
      assert te.email_status == :active

      mbox = Proca.TestEmailBackend.mailbox(email)

      assert length(mbox) == 20

      msg = List.first(mbox)
      assert %{"Reply-To" => _} = msg.headers
    end
  end

  test "preserving email and last name for MTTs", %{campaign: c, org: org} do
    assert Proca.Pipes.Connection.is_connected?()

    preview_ap = Factory.insert(:action_page, campaign: c, org: org, live: false)
    live_ap = Factory.insert(:action_page, campaign: c, org: org, live: true)

    test_for = fn ap ->
      supporter = Factory.insert(:basic_data_pl_supporter_with_contact, action_page: ap)

      assert supporter.email != nil
      assert supporter.last_name != nil

      action = Factory.insert(:action, supporter: supporter)

      process(action)

      action = Proca.Repo.reload(action)
      assert action.processing_status == :delivered

      supporter = Proca.Repo.reload(supporter)
      assert supporter.email != nil
      assert supporter.last_name != nil
    end

    test_for.(preview_ap)
    test_for.(live_ap)
  end

  test "sending without template", %{campaign: c, targets: [t | _]} do
    msg = Factory.insert(:message, target: t)

    MTTWorker.send_emails(c, [msg])

    [%{email: target_email}] = t.emails

    [email] = TestEmailBackend.mailbox(target_email)

    assert String.starts_with?(email.html_body, "<p>MTT text body to #{t.name}")
    assert String.starts_with?(email.subject, "MTT Subject to #{t.name}")
  end

  test "sending with local template", %{org: org, campaign: c, ap: page, targets: [t | _]} do
    import Proca.Repo

    msg = Factory.insert(:message, target: t)

    template =
      insert!(
        Proca.Service.EmailTemplate.changeset(%{
          org: org,
          name: "local_mtt",
          locale: "en",
          subject: "{{subject}}",
          html: """
          {{{body}}}

          <p>Sent in {{campaign.title}} campaign</p>
          """
        })
      )

    update!(Proca.MTT.changeset(c.mtt, %{message_template: template.name}))

    c = Proca.Campaign.one(id: c.id, preload: [:mtt, :org])

    MTTWorker.send_emails(c, [msg])

    [%{email: target_email}] = t.emails

    [email] = TestEmailBackend.mailbox(target_email)

    assert email.subject == msg.message_content.subject
    assert String.contains?(email.html_body, "Sent in Petition about")
    assert email.private[:custom_id] == "mtt:#{msg.id}"
  end

  describe "sending more emails than limit" do
    setup %{campaign: c, ap: ap, targets: [t1, t2 | _]} do
      actions =
        Factory.insert_list(200, :action,
          action_page: ap,
          processing_status: :delivered,
          supporter_processing_status: :accepted
        )

      msgs1 = Enum.map(actions, &Factory.insert(:message, action: &1, target: t1))
      msgs2 = Enum.map(actions, &Factory.insert(:message, action: &1, target: t2))

      Proca.Server.MTT.dupe_rank()

      %{
        actions: actions,
        messages: List.flatten([msgs1, msgs2]),
        targets: [t1, t2]
      }
    end

    test "don't return more mtt than limit", %{
      actions: actions,
      campaign: c,
      targets: [%{id: tid1}, %{id: tid2}],
      messages: msgs
    } do
      emails = MTTWorker.get_emails_to_send([tid1, tid2], {700, 700})
      assert length(emails) == 99
    end
  end
end
