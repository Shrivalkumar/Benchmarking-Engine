import mongoose, { Schema, Document } from 'mongoose';

export interface IUser extends Document {
  username: string;
  passwordHash: string;
  teamName: string;
  contestantId: number;
  createdAt: Date;
}

const UserSchema: Schema = new Schema({
  username: { type: String, required: true, unique: true, trim: true, lowercase: true },
  passwordHash: { type: String, required: true },
  teamName: { type: String, required: true, trim: true },
  contestantId: { type: Number, required: true },
  createdAt: { type: Date, default: Date.now }
});

export default mongoose.model<IUser>('User', UserSchema);
