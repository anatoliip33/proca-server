defmodule Proca.Server.MTTWorker do
  @moduledoc """
  MTT worker sends MTT emails for particular campaign.

  It will only send emails in the time window, defined by:
  - `campaign.mtt.startAt` {start_day, start_time}
  - `campaign.mtt.endAt` {end_day, end_time}
  - we are in time window only if `start_day <= today <= end_day` AND `start_time <= current_time <= end_time`.

  In each cycle (when run) MTTWorker calculates how many cycles has already passed (PASSED) and how many remain until end of sending (REMAIN).
  It then checks, if we already sent `ALL_EMAILS_FOR_TARGET*PASSED/(PASSED+REMAINED)` emails, and if not, it sends enough emails to that target to match the proportion.
  This way the MTT sending is spread linearly throughout all sending period.

  Worker will only send to targets, who have a good email (with `emailStatus`=`NONE`, not bouncing).

  The test emails are sent separately and instantly, and only one email is sent
  per supporter (so testing emails to 10 targets will just send 1 email not to
  spam while testing).

  """

  alias Proca.Repo
  import Ecto.Query

  alias Swoosh.Email
  alias Proca.{Action, Campaign, ActionPage, Org, TargetEmail}
  alias Proca.Action.Message
  alias Proca.Service.{EmailBackend, EmailTemplate, EmailMerge, EmailTemplateDirectory}
  import Proca.Stage.Support, only: [camel_case_keys: 1]

  require Logger

  @default_locale "en"
  @recent_test_messages -1 * 60 * 60 * 24

  def process_mtt_campaign(campaign) do
    campaign = Repo.preload(campaign, [:mtt, [org: :email_backend]])

    if campaign.org.email_backend != nil and within_sending_window(campaign) do
      # `all_cycles` is the total number of sending hours for the entire MTT campaign.
      # `cycle` is the number of sending hours that have passed so far.
      # The ratio of `cycle` to `all_cycles` determines what percentage of total
      # emails should have been sent by this point, ensuring a linear send rate over the campaign's duration.
      {cycle, all_cycles} = calculate_cycles(campaign)
      target_ids = get_sendable_target_ids(campaign)
      limit_per_hour = max_messages_per_hour(campaign)

      :telemetry.execute(
        [:proca, :mtt],
        %{sendable_targets: length(target_ids), current_cycle: cycle, all_cycles: all_cycles},
        %{campaign_id: campaign.id, campaign_name: campaign.name}
      )

      Logger.info(
        "MTT worker #{campaign.name}: #{length(target_ids)} targets, send cycle #{cycle}/#{all_cycles}"
      )

      # Send via central campaign.org.email_backend
      Enum.chunk_every(target_ids, 10)
      |> Enum.each(fn target_ids ->
        emails_to_send = get_emails_to_send(target_ids, {cycle, all_cycles}, limit_per_hour)

        Logger.info(
          "MTT worker #{campaign.name}: Sending #{length(emails_to_send)} emails for chunk of targets: #{inspect(target_ids)}, cycle #{cycle}/#{all_cycles}"
        )

        :telemetry.execute(
          [:proca, :mtt],
          %{messages_sent: length(emails_to_send)},
          %{campaign_id: campaign.id, campaign_name: campaign.name}
        )

        send_emails(campaign, emails_to_send)
      end)

      # Alternative:
      # send via each action page owner
    else
      if campaign.org.email_backend == nil do
        Logger.error(
          "MTT #{campaign.name} cannot send because #{campaign.org.name} org does not have an email backend"
        )
      end

      :noop
    end
  end

  def process_mtt_test_mails() do
    # Purge old test messages
    Repo.delete_all(query_test_emails_to_delete())

    # Get recent messages to send
    emails =
      get_test_emails_to_send()
      |> Enum.group_by(& &1.action.campaign_id)

    # Group per campaign and send
    for {campaign_id, ms} <- emails do
      campaign = Campaign.one(id: campaign_id, preload: [:mtt, org: :email_backend])

      if campaign.org.email_backend != nil do
        send_emails(campaign, ms)
        {campaign_id, length(ms)}
      else
        {campaign_id, 0}
      end
    end
  end

  def get_test_emails_to_send() do
    # lets just send recent messages
    recent = DateTime.utc_now() |> DateTime.add(@recent_test_messages, :second)

    Message.select_by_targets(:all, false, true)
    |> where([m, t, a], a.inserted_at >= ^recent)
    |> order_by([m, t, a], asc: m.id)
    |> preload([m, t, a], [[target: :emails], [action: :supporter], :message_content])
    |> Repo.all()
  end

  @doc """
  Query for test emails which were sent, and were created day ago.
  """
  def query_test_emails_to_delete() do
    recent = DateTime.utc_now() |> DateTime.add(@recent_test_messages, :second)

    from m in Message,
      join: a in assoc(m, :action),
      where: a.processing_status == :delivered and a.testing and m.sent and a.inserted_at < ^recent
  end

  @doc """
  Queries for targets with at least one email that is sendable (status none)
  Returns list of ids
  """
  def get_sendable_target_ids(%Campaign{id: id}) do
    from(t in Proca.Target,
      join: c in assoc(t, :campaign),
      join: te in assoc(t, :emails),
      where: c.id == ^id and te.email_status in [:active, :none],
      distinct: t.id,
      select: t.id
    )
    |> Repo.all()
  end

  @doc """
  Calculates the campaign's progress in terms of sending "cycles".

  A "cycle" corresponds to one hour within the allowed daily sending window,
  as defined by the campaign's `start_at` and `end_at` times.

  This function returns a tuple `{current_cycle, total_cycles}` where:
  - `total_cycles` is the total number of sending hours for the entire duration
    of the campaign.
  - `current_cycle` is the number of sending hours that have already passed
    since the campaign started.

  The ratio of `current_cycle` to `total_cycles` is used to determine the
  proportion of emails that should have been sent by the current time,
  ensuring a linear distribution of emails over the campaign's lifetime.

  ### Example

  Campaign has the following settings:
  - `start_at`: `~U[2025-09-05 09:00:00Z]`
  - `end_at`: `~U[2025-09-07 17:00:00Z]`

  And the current time is `~U[2025-09-05 11:46:00Z]`.

  The function calculates progress by determining how many cycles are *left*
  and subtracting that from the total.

  - The sending window is 8 hours/day (from 09:00 to 17:00). The campaign runs
    for 3 days. `total_cycles` will be `3 * 8 = 24`.
  - There is 1 full day remaining after today. That's `1 * 8 = 8` cycles.
  - In the current day, the time is 11:30. The window ends at 17:00. The number
    of full hours remaining is `floor(17:00 - 11:30) = 5` cycles.
  - Total cycles remaining = `8 + 5 = 13`.
  - `current_cycle` = `total_cycles - cycles_remaining` = `24 - 13 = 11`.

  The function would return `{3, 24}`.
  """
  def calculate_cycles(
        %Campaign{mtt: %{start_at: start_at, end_at: end_at}},
        now \\ DateTime.utc_now()
      ) do
    # cycles run in office hours, not 24h, per day it is:
    cycles_per_day = calculate_cycles_in_day(start_at, end_at)
    # cycles left today:
    cycles_today = calculate_cycles_in_day(now, end_at)

    # add +1 to count current day. We validated end_at > start_at, so it's 1 day even if it's a 5 minute one
    all_days = Date.diff(end_at, start_at) + 1
    # how many days behind us
    days_left = Date.diff(end_at, now)

    total_cycles = all_days * cycles_per_day

    {
      # cycles left are rounded now, so by substracting we will round up - so we are not left with any messages at the end of schedule
      total_cycles - (days_left * cycles_per_day + cycles_today),
      total_cycles
    }
  end

  defp calculate_cycles_in_day(start_time, end_time) do
    Time.diff(end_time, start_time, :hour)
  end

  @doc """
  Check if we are now in sending days, and also in sending hours.

  The Server.MTT select only campaigns in day-window already, but it is worth to double check here.
  """
  def within_sending_window(%{mtt: %{start_at: start_at, end_at: end_at}}) do
    now = DateTime.utc_now()

    in_sending_days =
      DateTime.compare(now, start_at) == :gt and DateTime.compare(end_at, now) == :gt

    start_time = DateTime.to_time(start_at)
    end_time = DateTime.to_time(end_at)

    in_sending_time = Time.compare(now, start_time) == :gt and Time.compare(end_time, now) == :gt

    in_sending_days and in_sending_time
  end

  def get_emails_to_send(target_ids, {cycle, all_cycles}, limit_per_hour) do
    # Subquery to count delivered/goal messages for each target
    progress_per_target =
      Message.select_by_targets(target_ids, [false, true])
      |> select([m, t, a], %{
        target_id: t.id,
        goal: count(m.id) * ^cycle / ^all_cycles,
        sent: fragment("count(?) FILTER (WHERE sent)", m.id)
      })
      |> group_by([m, t, a], t.id)

    # Subquery to rank unsent message ids and select only these need to meet current goal
    unsent_per_target_ids =
      Message.select_by_targets(target_ids, false)
      |> select([m, t, a], %{
        message_id: m.id,
        target_id: t.id,
        rank: fragment("RANK() OVER (PARTITION BY ? ORDER BY ?)", t.id, m.id)
      })
      |> subquery()
      |> join(:inner, [r], p in subquery(progress_per_target), on: r.target_id == p.target_id)
      # <= because rank is 1-based
      |> where([r, p], p.sent + r.rank <= p.goal)
      |> select([r, p], r.message_id)
      |> limit(^limit_per_hour)

    # Finally, fetch these messages with associations in one go
    Repo.all(
      from(m in Message,
        where: m.id in subquery(unsent_per_target_ids),
        preload: [[target: :emails], [action: :supporter], :message_content]
      )
    )
  end

  def send_emails(campaign, msgs) do
    org = Org.one(id: campaign.org_id, preload: [:email_backend, :storage_backend])

    # fetch action pages for email merge
    action_pages_ids =
      msgs
      |> Enum.map(fn m -> m.action.action_page_id end)

    action_pages =
      ActionPage.all(preload: [:org], by_ids: action_pages_ids)
      |> Enum.into(%{})

    msgs_per_locale = Enum.group_by(msgs, &(&1.target.locale || @default_locale))

    target_locales = Enum.uniq(Map.keys(msgs_per_locale))

    Sentry.Context.set_extra_context(%{
      campaign_id: campaign.id,
      campaign_name: campaign.name,
      org_id: org.id
    })

    templates =
      Enum.map(target_locales, fn locale ->
        case EmailTemplateDirectory.by_name_reload(org, campaign.mtt.message_template, locale) do
          {:ok, t} ->
            {locale, t}

          err when err in [:not_found] ->
            {locale, nil}
        end
      end)
      |> Enum.into(%{})

    for {locale, msgs} <- msgs_per_locale do
      # for testing, just send the first one
      msgs =
        case msgs do
          [%{action: %{testing: true}} = m | rest] ->
            {testing, real} = Enum.split_with(rest, & &1.action.testing)
            # Message.mark_all(testing, :sent)
            Message.mark_all(testing, :delivered)
            [m | real]

          ms ->
            ms
        end

      for chunk <- Enum.chunk_every(msgs, EmailBackend.batch_size(org)) do
        batch =
          for e <- chunk do
            Sentry.Context.set_extra_context(%{action_id: e.action_id})

            message_content = change_test_subject(e.message_content, e.action.testing)

            e
            |> make_email(campaign.mtt.test_email)
            |> EmailMerge.put_action_page(action_pages[e.action.action_page_id])
            |> EmailMerge.put_campaign(campaign)
            |> EmailMerge.put_action(e.action)
            |> EmailMerge.put_target(e.target)
            |> EmailMerge.put_files(resolve_files(org, e.files))
            |> put_message_content(message_content, templates[locale])
          end

        case EmailBackend.deliver(batch, org, templates[locale]) do
          :ok ->
            batch
            |> Enum.flat_map(fn m ->
              case m.private.email_id do
                nil ->
                  []

                id ->
                  [id]
              end
            end)
            |> TargetEmail.mark_all(:active)

            Message.mark_all(chunk, :sent)

          {:error, statuses} ->
            Logger.error("MTT failed to send, statuses: #{inspect(statuses)}")

            Enum.zip(chunk, statuses)
            |> Enum.filter(fn
              {_, :ok} -> true
              _ -> false
            end)
            |> Enum.map(fn {m, _} -> m end)
            |> Message.mark_all(:sent)
        end
      end
    end
  end

  def make_email(
        message = %{id: message_id, action: %{supporter: supporter, testing: is_test}},
        test_email \\ nil
      ) do
    email_to =
      if is_test do
        %Proca.TargetEmail{email: supporter.email, email_status: :none}
      else
        Enum.find(message.target.emails, fn email_to ->
          email_to.email_status in [:active, :none]
        end)
      end

    # Re-use logic to convert first_name, last_name to name
    supporter_name =
      Proca.Contact.Input.Contact.normalize_names(Map.from_struct(supporter))[:name]

    Proca.Service.EmailBackend.make_email(
      {message.target.name, email_to.email},
      {:mtt, message_id},
      email_to.id
    )
    |> Email.from({supporter_name, supporter.email})
    |> maybe_add_cc(test_email, is_test)
  end

  def resolve_files(org, file_keys) do
    case Proca.Service.fetch_files(org, file_keys) do
      {:ok, files} -> files
      {:error, _reason, partial} -> partial
    end
  end

  def put_message_content(
        email = %Email{},
        %Action.MessageContent{subject: subject, body: body},
        _template = nil
      ) do
    # Render the raw body
    target_assigns = camel_case_keys(%{target: email.assigns[:target]})

    Sentry.Context.set_extra_context(%{
      template_id: nil,
      template_name: nil,
      message_subject: subject,
      message_body: body
    })

    body =
      body
      |> EmailTemplate.compile_string()
      |> EmailTemplate.render_string(target_assigns)

    subject =
      subject
      |> EmailTemplate.compile_string()
      |> EmailTemplate.render_string(target_assigns)

    html_body = Proca.Service.EmailMerge.plain_to_html(body)

    email
    |> Email.html_body(html_body)
    |> Email.text_body(body)
    |> Email.subject(subject)
  end

  def put_message_content(
        email = %Email{},
        %Action.MessageContent{subject: subject, body: body},
        _template
      ) do
    html_body = Proca.Service.EmailMerge.plain_to_html(body)

    email
    |> Email.assign(:body, html_body)
    |> Email.assign(:subject, subject)
  end

  defp change_test_subject(message_content, false), do: message_content

  defp change_test_subject(message_content = %{subject: subject}, true) do
    Map.put(message_content, :subject, "[TEST] " <> subject)
  end

  defp maybe_add_cc(email, cc, true), do: Email.cc(email, cc)
  defp maybe_add_cc(email, _cc, false), do: email

  def max_messages_per_hour(%Campaign{mtt: %{max_messages_per_hour: _, timezone: nil}} = campaign) do
    mtt = %{campaign.mtt | timezone: "Etc/UTC"}
    campaign = %{campaign | mtt: mtt}

    max_messages_per_hour(campaign)
  end

  def max_messages_per_hour(%Campaign{mtt: %{max_messages_per_hour: nil, timezone: _}} = campaign) do
    limit_messages_per_hour =
      Application.get_env(:proca, Proca.Server.MTTWorker)
      |> Access.get(:max_emails_per_hour, 30)

    mtt = %{campaign.mtt | max_messages_per_hour: limit_messages_per_hour}
    campaign = %{campaign | mtt: mtt}

    max_messages_per_hour(campaign)
  end

  def max_messages_per_hour(%Campaign{mtt: %{max_messages_per_hour: max_messages_per_hour, timezone: timezone}}) do
    Application.get_env(:proca, Proca.Server.MTTWorker)
    |> Access.get(:messages_ratio_per_hour)
    |> Access.get(DateTime.now!(timezone).hour)
    |> Kernel.*(max_messages_per_hour)
    |> trunc()
    |> max(1)
  end
end
