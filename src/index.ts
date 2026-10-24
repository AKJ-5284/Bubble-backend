import { WebSocket, WebSocketServer, type RawData } from "ws";
import { randomUUID } from "crypto";

type SocketData = {
    userId : string;
    userName : string;
    roomId : string;
}

type ClientMessage = {
    type: "join";
    roomId : string;
}|{
    type : "chat";
    text : string;
}|{
    type : "members"
}

type ChatMessage = {
    type : "chat";
    roomId : string;
    userId : string;
    userName : string;
    text : string;
    timestamp : number;
}

type MembersMessage = {
    type: "members";
    members: Array<{
        userId: string;
        userName: string;
    }>;
};

const socketData = new WeakMap<WebSocket,SocketData>(); //socket meta data
const rooms = new Map<string, Set<WebSocket>>(); //each room has roomid and bunch of sockets
const port = Number(process.env.PORT ?? 8080);
const wss = new WebSocketServer({ port });

function sendMemberNames(roomId : string){
    const room = rooms.get(roomId);
    if(!room) return;

    const membersMessage: MembersMessage = {
        type: "members",
        members: [...room]
            .map((client) => socketData.get(client))
            .filter((member): member is SocketData => Boolean(member))
            .map(({ userId, userName }) => ({ userId, userName })),
    };
    const stringifiedMessage = JSON.stringify(membersMessage);

    for(const client of room){
        if(client.readyState === WebSocket.OPEN){
            client.send(stringifiedMessage);
        }
    }
}

function isClientMessage(data: unknown): data is ClientMessage{
    if(typeof data !== "object" || data == null){
        return false;
    }
    const message = data as Record<string, unknown>
    if(message.type == "join"){
        return typeof message.roomId === "string" && message.roomId.trim().length > 0;
    }
    if(message.type == "chat"){
        return typeof message.text === "string" && message.text.trim().length > 0;
    }
    return message.type === "members";
}

function sendError(socket : WebSocket , message: string){
    if(socket.readyState === WebSocket.OPEN){
        socket.send(JSON.stringify({
            type : "error",
            message,
        }))
    }

}

function leaveRoom(socket: WebSocket){
    const data = socketData.get(socket);
    if(!data?.roomId)return;

    const room= rooms.get(data.roomId);
    if(!room) return;

    room.delete(socket);

    if(room.size === 0){
        rooms.delete(data.roomId);
    }

    data.roomId = "";

}

function joinRoom(socket: WebSocket, roomId : string){
    const data = socketData.get(socket);
    if(!data) return;

    //leave prev room
    leaveRoom(socket);

    if(!rooms.has(roomId)){
        rooms.set(roomId,new Set<WebSocket>);
    }
    rooms.get(roomId)?.add(socket);
    data.roomId = roomId;

    socket.send(JSON.stringify({
        type: "joined",
        roomId,
        userId: data.userId

    }))
    sendMemberNames(roomId);
    console.log(`${data.userName} joined ${roomId}`);
}

function broadcastToRoom(
    roomId : string,
    message : ChatMessage
){
    const room = rooms.get(roomId);
    if(!room){
        return
    }

    const stringifiedMsg = JSON.stringify(message);

    for(const client of room){
        if(client.readyState === WebSocket.OPEN){
            client.send(stringifiedMsg);
        }
    }
}




wss.on("connection",(socket,request) => {
    const url = new URL(request.url?? '/', "http://localhost" );
    const userName = url.searchParams.get("userName")?.trim() || "Anonymous";

    const data : SocketData = {
        userId : randomUUID(),
        userName : userName,
        roomId: "",
    }

    socketData.set(socket,data);

    console.log(`${data.userName} connected`);


    socket.on("message",(raw: RawData)=>{
        let parsed : unknown;
        try {
            parsed = JSON.parse(raw.toString());

        } catch (error) {
            console.log(error);
            sendError(socket,"Invalid JSON");
            return;
        }
        if(!isClientMessage(parsed)){
            sendError(socket,"Invalid message format");
            return;
        }
        if(parsed.type === "join"){
            joinRoom(socket,parsed.roomId.trim());
            return;
        }
        if(parsed.type === "chat"){
            const currentUser = socketData.get(socket);
            if(!currentUser?.roomId){
                sendError(socket,"Join a room first");
                return
            }
            const chatmsg : ChatMessage = {
                type : "chat",
                roomId : currentUser.roomId,
                userId : currentUser.userId,
                userName : currentUser.userName,
                text : parsed.text.trim(),
                timestamp: Date.now(),
            }
            broadcastToRoom(
                currentUser.roomId,
                chatmsg
            );
            return;
        }
        if(parsed.type === "members"){
            const currentUser = socketData.get(socket);
            if(!currentUser?.roomId){
                sendError(socket,"Join a room first");
                return;
            }
            sendMemberNames(currentUser.roomId);

        }
    })

    socket.on("close",()=>{
        leaveRoom(socket);
        socketData.delete(socket);
        console.log(`${data.userName} disconnected`);
    })
    socket.on("error",(error)=>{
        console.error(error.message);
    })

})

console.log(`WebSocket server listening on port ${port}`);