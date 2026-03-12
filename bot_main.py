import os
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import tasks

from db import (
    cleanup_expired_keys,
    create_or_replace_key_for_user,
    get_active_key_for_user,
    get_db_health,
    init_db,
)

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


@tree.command(name="key", description="Get your Shenanigans key (private reply)")
async def key_command(interaction: discord.Interaction):
    user_id = interaction.user.id
    await interaction.response.defer(ephemeral=True)

    try:
        existing = get_active_key_for_user(user_id)

        if existing:
            expiry = existing["expires_at"]
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            remaining = expiry - now
            total_seconds = max(int(remaining.total_seconds()), 0)
            hours, remainder = divmod(total_seconds, 3600)
            minutes = remainder // 60

            await interaction.followup.send(
                f"You already have an active key!\n"
                f"**{existing['key_value']}**\n"
                f"Expires in {hours}h {minutes}m ({expiry.strftime('%Y-%m-%d %H:%M UTC')})",
                ephemeral=True,
            )
            return

        created = create_or_replace_key_for_user(user_id, hours_valid=24)
        expiry = created["expires_at"]
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)

        await interaction.followup.send(
            f"**Your Shenanigans key:**\n\n"
            f"{created['key_value']}\n\n"
            f"Expires: {expiry.strftime('%Y-%m-%d %H:%M UTC')} (24 hours)\n"
            f"Don't share this!",
            ephemeral=True,
        )

    except Exception as e:
        print(f"/key error for user {user_id}: {repr(e)}")
        await interaction.followup.send(
            "There was an error generating your key. Try again in a moment.",
            ephemeral=True,
        )


@tasks.loop(hours=1)
async def cleanup_loop():
    try:
        deleted = cleanup_expired_keys()
        if deleted:
            print(f"Cleaned {deleted} expired keys")
    except Exception as e:
        print(f"Cleanup error: {repr(e)}")


@cleanup_loop.before_loop
async def before_cleanup_loop():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    print(f"Bot logged in as {bot.user}")

    try:
        health = get_db_health()
        print(f"Database connection OK: {health}")
    except Exception as e:
        print(f"Database health check failed: {repr(e)}")

    try:
        init_db()
        print("Database initialized")
    except Exception as e:
        print(f"Database init failed: {repr(e)}")

    try:
        synced = await tree.sync()
        print(f"Synced {len(synced)} slash commands")
    except Exception as e:
        print(f"Slash sync failed: {repr(e)}")

    if not cleanup_loop.is_running():
        cleanup_loop.start()


def main():
    token = os.getenv("DISCORD_TOKEN")

    if not token:
        raise RuntimeError("DISCORD_TOKEN is missing")

    try:
        health = get_db_health()
        print(f"Startup DB connection OK: {health}")
    except Exception as e:
        print(f"Startup database check failed: {repr(e)}")

    try:
        init_db()
        print("Database initialized at startup")
    except Exception as e:
        print(f"Startup database init failed: {repr(e)}")

    bot.run(token)


if __name__ == "__main__":
    main()
