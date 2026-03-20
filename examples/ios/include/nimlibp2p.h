#ifndef NIMLIBP2P_H
#define NIMLIBP2P_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void nim_thread_attach(void);
void nim_thread_detach(void);

char *libp2p_get_last_error(void);
void libp2p_string_free(const char *value);

void *libp2p_node_init(const char *configJson);
int libp2p_node_start(void *handle);
int libp2p_node_stop(void *handle);
void libp2p_node_free(void *handle);
bool libp2p_node_is_started(void *handle);

char *libp2p_generate_identity_json(void);
char *libp2p_identity_from_seed(const uint8_t *seed, size_t seedLen);
char *libp2p_get_local_peer_id(void *handle);
char *libp2p_get_listen_addresses(void *handle);
char *libp2p_get_dialable_addresses(void *handle);
char *libp2p_get_connected_peers_json(void *handle);
char *libp2p_connected_peers_info(void *handle);
char *libp2p_poll_events(void *handle, int maxEvents);

char *libp2p_fetch_file_providers(void *handle, const char *key, int limit);
char *libp2p_register_local_file(void *handle, const char *key, const char *path, const char *metaJson);
char *libp2p_request_file_chunk(void *handle, const char *peerId, const char *requestJson, int maxBytes);
int libp2p_last_chunk_size(void *handle);

char *libp2p_get_local_system_profile_json(void *handle);
char *libp2p_get_network_resources_json(void *handle);
char *libp2p_refresh_node_resources(void *handle, int limit);
char *libp2p_network_discovery_snapshot(void *handle, const char *sourceFilter, int limit, int connectCap);

char *libp2p_game_mesh_create_room(
    void *handle,
    const char *appId,
    const char *remotePeerId,
    const char *conversationId,
    const char *seedAddrsJson,
    const char *matchSeed,
    bool sendInvite
);
char *libp2p_game_mesh_join_room(
    void *handle,
    const char *appId,
    const char *roomId,
    const char *hostPeerId,
    const char *conversationId,
    const char *seedAddrsJson,
    const char *matchId,
    const char *mode
);
char *libp2p_game_mesh_handle_incoming(
    void *handle,
    const char *peerId,
    const char *conversationId,
    const char *messageId,
    const char *payloadJson
);
char *libp2p_game_mesh_submit_action(void *handle, const char *appId, const char *roomId, const char *actionJson);
char *libp2p_game_mesh_current_state(void *handle, const char *appId, const char *roomId);
char *libp2p_game_mesh_wait_state(void *handle, const char *appId, const char *roomId, const char *expectedPhase, int timeoutMs);
char *libp2p_game_mesh_apply_script(void *handle, const char *appId, const char *roomId, const char *scriptName, int timeoutMs);
char *libp2p_game_mesh_apply_step(void *handle, const char *appId, const char *roomId);
char *libp2p_game_mesh_replay_verify(void *handle, const char *appId, const char *roomId);
char *libp2p_game_mesh_local_smoke(void *handle, const char *appId);

char *social_list_discovered_peers(void *handle, const char *sourceFilter, int limit);
bool social_connect_peer(void *handle, const char *peerId, const char *multiaddr);
bool social_dm_send(void *handle, const char *peerId, const char *conversationId, const char *messageJson);
bool social_dm_edit(void *handle, const char *peerId, const char *conversationId, const char *messageId, const char *patchJson);
bool social_dm_revoke(void *handle, const char *peerId, const char *conversationId, const char *messageId, const char *reason);
bool social_dm_ack(void *handle, const char *peerId, const char *conversationId, const char *messageId, const char *status);
bool social_contacts_send_request(void *handle, const char *peerId, const char *helloText);
bool social_contacts_accept(void *handle, const char *peerId);
bool social_contacts_reject(void *handle, const char *peerId, const char *reason);
bool social_contacts_remove(void *handle, const char *peerId);
char *social_groups_create(void *handle, const char *groupMetaJson);
bool social_groups_update(void *handle, const char *groupId, const char *patchJson);
bool social_groups_invite(void *handle, const char *groupId, const char *peerIdsJson);
bool social_groups_kick(void *handle, const char *groupId, const char *peerId);
bool social_groups_leave(void *handle, const char *groupId);
bool social_groups_send(void *handle, const char *groupId, const char *messageJson);
char *social_synccast_upsert_program(void *handle, const char *roomId, const char *programJson);
bool social_synccast_join(void *handle, const char *roomId, const char *peerId);
bool social_synccast_leave(void *handle, const char *roomId);
bool social_synccast_control(void *handle, const char *roomId, const char *controlJson);
char *social_synccast_get_state(void *handle, const char *roomId);
char *social_synccast_list_rooms(void *handle, int limit);
char *social_moments_publish(void *handle, const char *postJson);
bool social_moments_delete(void *handle, const char *postId);
bool social_moments_like(void *handle, const char *postId, bool like);
bool social_moments_comment(void *handle, const char *postId, const char *commentJson);
char *social_feed_snapshot(void *handle, const char *channel, const char *cursor, int limit);
char *social_content_detail(void *handle, const char *contentId);
char *social_playback_open(void *handle, const char *payloadJson);
char *social_playback_state(void *handle, const char *sessionId);
char *social_playback_control(void *handle, const char *payloadJson);
char *social_media_asset_status(void *handle, const char *assetId);
char *social_publish_enqueue(void *handle, const char *postJson);
char *social_publish_task(void *handle, const char *taskId);
char *social_publish_tasks(void *handle, const char *scope, const char *cursor, int limit);
char *social_notifications_list(void *handle, const char *cursor, int limit);
char *social_notifications_summary(void *handle);
char *social_notifications_mark(void *handle, const char *payloadJson);
char *social_subscriptions_set(void *handle, const char *payloadJson);
char *social_query_presence(void *handle, const char *peerIdsJson);
char *social_poll_events(void *handle, int maxEvents);
char *social_poll_events_without_dm(void *handle, int maxEvents);
char *social_received_direct_messages(void *handle, int limit);

#ifdef __cplusplus
}
#endif

#endif
